import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const python = path.join(root, 'bin/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
const fixture = JSON.parse(execFileSync(python, ['-c', `import asyncio,json,sys
sys.path.insert(0,'bin')
from test_lighthouse_signature_usage_workflows import SignatureUsageWorkflowTests, ACTOR
async def main():
 case=SignatureUsageWorkflowTests()
 case.setUp()
 try:
  raw=case.prepare()
  initial=case.portal.public_plan(raw)
  loaded=await case.portal.field_options(ACTOR, initial['id'], 'step0.signatures', case.request)
  roles={option['value']: ('implementer' if option['label'].endswith('维护实施人') else 'auditor' if option['label'].endswith('维护审核人') else 'inspector')
         for option in loaded['fields'][0].get('options', [])}
  choices=[option['value'] for option in loaded['fields'][0].get('options', [])]
  preview=case.portal.amend(ACTOR, initial['id'], {'version': loaded['version'], 'values': {'step0.signatures': choices}})
  print(json.dumps({'initial': initial, 'loaded': loaded, 'preview': preview, 'roles': roles}, ensure_ascii=False))
 finally:
  case.doCleanups()
asyncio.run(main())
`], { cwd: root, encoding: 'utf8', env: { ...process.env, PYTHONIOENCODING: 'utf-8' } }));
for (const key of ['initial', 'loaded', 'preview']) {
  const text = JSON.stringify(fixture[key]);
  for (const secret of ['private-file-token', 'NO-IMAGE-IN-CHAT', 'ou_person', '_references']) {
    assert.equal(text.includes(secret), false, key + ' leaked ' + secret);
  }
}
{
  const body = fixture.preview.operations[0].body;
  assert.deepEqual(body.signatures.map(s => s.role).sort(), ['auditor', 'implementer']);
  assert.equal(body.notice_title, 'A楼维护');
  assert.equal(body.mop_attachment_name, undefined);
  assert.equal(body.signatures.every(s => s.name === '签名人员甲' && s.record_id === 'person-a'), true);
  assert.equal(fixture.loaded.operations[0].selected_labels.selected_document, '维护单.xlsx');
}
const base = 'http://127.0.0.1:19003';
const output = path.join(root, 'output/playwright/assistant-signature-usage');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [], requests = [], amended = [];
    page.on('pageerror', error => errors.push(error.message));
    let plan = structuredClone(fixture.initial);
    const roles = fixture.roles;
    const loaded = structuredClone(fixture.loaded);
    const state = () => ({ ok: true, data: {
      conversation_id: 'sig-usage-fixture', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'sig-usage-fixture', question: '发送签名使用确认', answer: '请选择收件人', status: 'completed', plan }],
    } });
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: state() }));
    await page.route('**/api/assistant/plans/' + loaded.id + '/options?*', route => {
      const params = new URL(route.request().url()).searchParams;
      requests.push(Object.fromEntries(params));
      plan = structuredClone(loaded);
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.route('**/api/assistant/plans/' + loaded.id, route => {
      assert.equal(route.request().method(), 'PATCH');
      const body = route.request().postDataJSON();
      amended.push(body);
      plan = structuredClone(fixture.preview);
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    const form = page.locator('.plan-form');
    await form.waitFor();
    const continueBtn = form.getByRole('button', { name: '补充并继续', exact: true });
    assert.equal(await continueBtn.isDisabled(), true);
    await form.getByLabel('查找可选记录', { exact: true }).fill('甲');
    await form.getByRole('button', { name: '查找', exact: true }).click();
    await form.getByRole('checkbox', { name: /签名人员甲.*维护实施人/ }).waitFor();
    assert.equal(requests[0].q, '甲');
    assert.equal(await form.getByRole('checkbox').count(), 2);
    assert.equal(await form.getByRole('checkbox', { name: /签名人员甲.*维护审核人/ }).count(), 1);
    assert.equal(await continueBtn.isDisabled(), true);
    await form.getByRole('checkbox', { name: /签名人员甲.*维护实施人/ }).check();
    await form.getByRole('checkbox', { name: /签名人员甲.*维护审核人/ }).check();
    assert.equal(await continueBtn.isDisabled(), false);
    await page.keyboard.press('Escape');
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    await form.waitFor();
    assert.equal(await form.getByRole('checkbox', { name: /签名人员甲.*维护实施人/ }).isChecked(), true);
    assert.equal(await form.getByRole('checkbox', { name: /签名人员甲.*维护审核人/ }).isChecked(), true);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.screenshot({ path: path.join(output, `signature-usage-${width}-form.png`), fullPage: true });
    await continueBtn.click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    const steps = page.locator('.plan-steps');
    await steps.getByText('A楼维护', { exact: true }).waitFor();
    const recipientsLine = '签名人员甲（维护实施人）、签名人员甲（维护审核人）';
    await steps.getByText(recipientsLine, { exact: true }).waitFor();
    assert.deepEqual([...amended[0].values['step0.signatures']].sort(), Object.keys(roles).sort());
    const panelText = await page.locator('.assistant-panel').innerText();
    for (const expected of ['A楼维护', '维护单.xlsx', recipientsLine]) assert(panelText.includes(expected), 'missing ' + expected);
    for (const secret of ['private-file-token', 'notice-a', 'ou_person', 'ou_operator', 'NO-IMAGE-IN-CHAT', 'mop-a', 'person-a', 'value_']) assert(!panelText.includes(secret), 'leaked ' + secret);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.screenshot({ path: path.join(output, `signature-usage-${width}-preview.png`), fullPage: true });
    assert.deepEqual(errors, []);
    await context.close();
  }
  console.log('Assistant native signature-usage desktop/mobile OK');
} finally { await browser.close(); }