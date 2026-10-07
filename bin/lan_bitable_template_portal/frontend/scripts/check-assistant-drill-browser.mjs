import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const python = path.join(root, 'bin/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
const fixture = JSON.parse(execFileSync(python, ['-c', `import sys,json,tempfile
from pathlib import Path
sys.path.insert(0,'bin')
from test_drill_management import _fixture_xlsx
from test_lighthouse_stream import Store
from lan_bitable_template_portal.drill_management import DrillManagementService
from lan_bitable_template_portal.lighthouse_api import _drill_frontend_fields
from test_lighthouse_drill_configuration_workflows import DrillConfigurationWorkflowTests
with tempfile.TemporaryDirectory() as temp:
 service=DrillManagementService(Store(Path(temp)/'state.sqlite3'),data_root=Path(temp)/'drills')
 definition=service.create_definition(name='isolated',year=2026,month=10,file_name='fixture.xlsx',source=_fixture_xlsx(with_evaluator=True))
 definition['configuration']['steps'][0]['signature_slots']=2
 people=[{'record_id':'person-'+str(i),'name':'人员'+str(i),'employee_no':'EMP-'+str(i),'building':'B','has_signature':True} for i in range(12)]
 value={'expected_version':3,'drill_date':'2026-10-02','first_start_time':'09:30','simulation_scenario':'原始场景','commander':people[0],'participants':people[:3],'evaluator':people[3],'signature_time':'2026-10-02T10:15','evaluation_time':'2026-10-02T11:32','step_signers':{str(s['row']):(['person-1','person-0']+['']*s['signature_slots'])[:s['signature_slots']] for s in definition['configuration']['steps']}}
 control=_drill_frontend_fields(definition,value,people)
 case=DrillConfigurationWorkflowTests()
 case.setUp()
 try:
  config_plan=case.portal.public_plan(case.prepare())
  print(json.dumps({'control':control,'value':value,'configuration':config_plan},ensure_ascii=False))
 finally:
  case.doCleanups()
`], { cwd: root, env: { ...process.env, PYTHONIOENCODING: 'utf-8' }, encoding: 'utf8' }));
const output = path.join(root, 'output/playwright/assistant-drill');
await mkdir(output, { recursive: true });
const base = 'http://127.0.0.1:19003';
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [], saved = [];
    page.on('pageerror', error => errors.push(error.message));
    let plan = { id: 'drill-fixture', status: 'needs_input', version: 1, title: '演练填写', fields: [{ ...fixture.control, name: 'execution', path: '', label: '演练填写', value: fixture.value }], operations: [], results: [] };
    const initialFields = structuredClone(plan.fields);
    let editCalls = 0;
    const conversation = () => ({ ok: true, data: { conversation_id: 'drill-fixture', configured: true, enabled: true, busy: false, turns: [{ operation_id: 'drill-fixture', question: '填写演练', answer: '请填写', status: 'completed', plan }] } });
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: conversation() }));
    await page.route('**/api/assistant/plans/drill-fixture', route => {
      assert.equal(route.request().method(), 'PATCH');
      const body = route.request().postDataJSON();
      assert.equal(body.version, plan.version);
      if (body.action === 'edit') {
        editCalls++;
        plan = { ...plan, fields: initialFields.map(field => ({ ...field, value: structuredClone(saved.at(-1).values.execution) })), status: 'needs_input', can_edit: false, version: plan.version + 1 };
      } else {
        saved.push(body);
        plan = { ...plan, fields: [], status: 'awaiting_confirmation', can_edit: true, version: plan.version + 1 };
      }
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.route('**/api/assistant/plans/drill-fixture/confirm', route => {
      assert.equal(route.request().postDataJSON().stage, 'review');
      plan = { ...plan, status: 'awaiting_second_confirmation', version: plan.version + 1 };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    const form = page.locator('.plan-form');
    await form.waitFor();
    await form.getByLabel('演练审核人签名时间', { exact: true }).fill('2026-10-02T10:30');
    assert.equal(await form.getByLabel('演练评估人评估时间', { exact: true }).inputValue(), '2026-10-02T11:32');
    const participants = form.getByRole('group', { name: '参演人员（含指挥人）', exact: true });
    assert.equal(await participants.getByRole('combobox').first().isDisabled(), true);
    const firstStep = form.getByRole('group', { name: '第 13 行 · ECC · 第一步', exact: true });
    assert.equal(await firstStep.getByLabel('执行人', { exact: true }).count(), 2);
    assert.equal(await firstStep.getByRole('button').count(), 0);
    await form.getByRole('combobox', { name: '指挥人', exact: true }).click();
    assert.equal(await form.getByRole('combobox', { name: '指挥人', exact: true }).evaluate(el => getComputedStyle(el).backgroundColor), 'rgb(41, 50, 44)');
    await page.getByPlaceholder('搜索指挥人', { exact: true }).fill('EMP-4');
    const menu = page.locator('.vnet-select-menu');
    assert.equal(await menu.evaluate(element => Number(getComputedStyle(element).zIndex)), 10010);
    assert.equal(await menu.evaluate(el => getComputedStyle(el).backgroundColor), 'rgb(35, 42, 37)', 'teleported menu must retain the assistant theme');
    assert.equal(await menu.getByRole('option').first().evaluate(el => getComputedStyle(el).color), 'rgb(226, 232, 226)');
    await page.screenshot({ path: path.join(output, `drill-menu-${width}.png`), fullPage: true });
    await menu.getByRole('option', { name: '人员4 · EMP-4 · B', exact: true }).click();
    assert.equal(await firstStep.getByLabel('执行人', { exact: true }).nth(1).inputValue(), '');
    const select = firstStep.getByLabel('执行人', { exact: true }).nth(1);
    const available = await select.locator('option').allTextContents();
    assert(available.includes('人员4'));
    assert(!available.includes('人员0'));
    await select.selectOption({ label: '人员4' });
    await participants.getByRole('button', { name: '添加参演人员（含指挥人）', exact: true }).click();
    await participants.getByRole('combobox').last().click();
    await page.getByPlaceholder('搜索参演人', { exact: true }).fill('EMP-5');
    await page.getByRole('option', { name: '人员5 · EMP-5 · B', exact: true }).click();
    assert.equal(await participants.getByRole('combobox').count(), 4);
    await participants.getByRole('button', { name: '删除第 2 项', exact: true }).click();
    assert.equal(await firstStep.getByLabel('执行人', { exact: true }).first().inputValue(), '');
    assert.equal(await firstStep.getByLabel('执行人', { exact: true }).nth(1).inputValue(), 'person-4');
    await form.getByLabel('演练评估人评估时间', { exact: true }).fill('2026-10-02T12:45');
    await form.getByLabel('模拟场景', { exact: true }).fill('更改场景但不改模板');
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.screenshot({ path: path.join(output, `drill-${width}.png`), fullPage: true });
    await form.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.equal(saved.length, 1);
    const value = saved[0].values.execution;
    assert.deepEqual(value.participants.map(person => person.record_id), ['person-4', 'person-2', 'person-5']);
    assert.deepEqual(value.step_signers['13'], ['', 'person-4']);
    assert.equal(value.expected_version, 3);
    assert.equal(value.signature_time, '2026-10-02T10:30');
    assert.equal(value.evaluation_time, '2026-10-02T12:45');
    assert.equal(value.simulation_scenario, '更改场景但不改模板');
    await page.getByRole('button', { name: '确认操作清单', exact: true }).click();
    await page.getByRole('button', { name: '再次确认并执行', exact: true }).waitFor();
    await page.getByRole('button', { name: '返回修改', exact: true }).click();
    await form.waitFor();
    assert.equal(editCalls, 1);
    assert.equal(await form.getByLabel('演练审核人签名时间', { exact: true }).inputValue(), '2026-10-02T10:30');
    assert.equal(await form.getByLabel('演练评估人评估时间', { exact: true }).inputValue(), '2026-10-02T12:45');
    await page.reload();
    await form.waitFor();
    assert.equal(await form.getByLabel('模拟场景', { exact: true }).inputValue(), '更改场景但不改模板');
    assert.equal(await participants.getByRole('combobox').count(), 3);
    await form.getByLabel('演练评估人评估时间', { exact: true }).fill('2026-10-02T13:45');
    await form.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.equal(saved.length, 2);
    assert.equal(saved[1].values.execution.evaluation_time, '2026-10-02T13:45');
    assert.equal(saved[1].values.execution.expected_version, 3);
    assert.deepEqual(saved[1].values.execution.participants, value.participants);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.screenshot({ path: path.join(output, `drill-edit-review-${width}.png`), fullPage: true });
    assert.deepEqual(errors, []);
    await context.close();
  }
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    const page = await context.newPage(), errors = [], saved = [];
    page.on('pageerror', error => errors.push(error.message));
    let plan = structuredClone(fixture.configuration);
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: { ok: true, data: {
      conversation_id: 'configuration-fixture', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'configuration-fixture', question: '修改演练模板配置', answer: '请核对', status: 'completed', plan }],
    } } }));
    await page.route('**/api/assistant/plans/' + plan.id, route => {
      saved.push(route.request().postDataJSON());
      plan = { ...plan, fields: [], version: 2, status: 'awaiting_confirmation', operations: [{ ...plan.operations[0], selected_labels: {
        name: '模板演练', record_sheet: '本月记录', assessment_sheet: '评估表', step_slots: '第 13 行 2 人',
      } }] };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    const form = page.locator('.plan-form');
    await form.waitFor();
    const layout = form.getByRole('group', { name: '步骤表格映射', exact: true });
    const record = form.getByRole('group', { name: '记录表字段映射', exact: true });
    const assessment = form.getByRole('group', { name: '评估表字段映射', exact: true });
    const people = form.getByRole('group', { name: '步骤执行人签名人数', exact: true });
    assert.equal(await form.getByLabel('演练记录表', { exact: true }).inputValue(), '本月记录');
    assert.equal(await layout.getByLabel('位置列', { exact: true }).locator('option').count(), 81);
    assert.equal(await layout.getByLabel('位置列', { exact: true }).locator('option').last().textContent(), 'CB');
    assert.equal(await record.getByLabel('审核人签名区', { exact: true }).inputValue(), 'C18:E18');
    assert.equal(await assessment.getByLabel('评估人签名区', { exact: true }).inputValue(), 'D12:F12');
    await page.screenshot({ path: path.join(output, `configuration-top-${width}.png`), fullPage: true });
    await people.getByLabel('查找步骤执行人签名人数', { exact: true }).fill('ECC');
    const slots = people.getByLabel('第 13 行 · ECC · 第一步', { exact: true });
    assert.equal(await slots.getAttribute('type'), 'number');
    assert.equal(await slots.getAttribute('max'), '10');
    await slots.fill('2');
    await record.getByLabel('演练审核人签名时间', { exact: true }).fill('G18:H18');
    assert.equal(await form.locator('textarea').count(), 0);
    assert.equal(await form.locator('img').count(), 0);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await slots.scrollIntoViewIfNeeded();
    await page.screenshot({ path: path.join(output, `configuration-steps-${width}.png`), fullPage: true });
    await form.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.equal(saved.length, 1);
    assert.equal(saved[0].values['step0.configuration'].signers.step_0, 2);
    assert.equal(saved[0].values['step0.configuration'].mapping.review_time, 'G18:H18');
    assert.equal(saved[0].values['step0.configuration'].assessment.cell_0, 'D12:F12');
    await page.getByText('第 13 行 2 人', { exact: true }).waitFor();
    assert.equal(await page.locator('.operation-preview').getByText('configuration', { exact: true }).count(), 0);
    assert.deepEqual(errors, []);
    await context.close();
  }
  console.log('Assistant native drill fields desktop/mobile OK');
} finally { await browser.close(); }
