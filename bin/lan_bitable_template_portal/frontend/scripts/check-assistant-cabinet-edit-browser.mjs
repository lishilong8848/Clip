import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const python = path.join(root, 'bin/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
const field = JSON.parse(execFileSync(python, ['-c', `import sys,json
sys.path.insert(0,'bin')
from lan_bitable_template_portal.lighthouse_agent import _cabinet_edit_field
rows=[{'row_id':'row.'+str(i),'scope':'E','room':'202','rack':rack,'editable':True,'status':'ready','current_power_state':'off','action':'上正式电','actual':'2026-09-22 13:05:32','expected':'2026-09-22 14:00:00','result':'成功','evidence':[{'file_token':'private-proof'}]} for i,rack in enumerate(('A11','A12'))]
_,field=_cabinet_edit_field({'scopes':['E']},{'api_id':'PATCH /api/cabinet-power/batches/{batch_id}','path_params':{'batch_id':'isolated'}},{'query':{'batch_id':'isolated','version':5,'source':'image','rows':rows}},0)
field['value']=field.pop('_initial_form')
field.pop('_baseline')
print(json.dumps(field,ensure_ascii=False))
`], { cwd: root, encoding: 'utf8', env: { ...process.env, PYTHONIOENCODING: 'utf-8' } }));
const base = 'http://127.0.0.1:19003';
const output = path.join(root, 'output/playwright/assistant-cabinet-edit');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), writes = [], errors = [];
    page.on('pageerror', error => errors.push(error.message));
    let plan = { id: 'cabinet-edit', version: 1, status: 'needs_input', title: '更正机柜待办', fields: [field], operations: [], results: [] };
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: { ok: true, data: {
      conversation_id: 'cabinet-edit', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'cabinet-edit', question: '更正这个机柜批次', status: 'completed', answer: '请核对下方填写项。', plan }],
    } } }));
    await page.route('**/api/assistant/plans/cabinet-edit', route => {
      assert.equal(route.request().method(), 'PATCH');
      writes.push(route.request().postDataJSON());
      plan = { ...plan, fields: [], status: 'awaiting_confirmation', version: 2 };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    const form = page.locator('.plan-form');
    await form.getByRole('group', { name: 'E楼 202/A11', exact: true }).waitFor();
    assert.equal(await form.getByRole('group', { name: 'E楼 202/A12', exact: true }).count(), 0);
    assert.equal(await form.getByLabel('实际完成时间', { exact: true }).inputValue(), '2026-09-22T13:05:32');
    assert.equal(await form.getByLabel('楼栋', { exact: true }).locator('option[value="A"]').count(), 0);
    const action = form.getByLabel('操作类型', { exact: true });
    assert.equal(await action.locator('option[value="下正式电"]').count(), 0);
    await action.selectOption('上测试电');
    await form.getByLabel('实际完成时间', { exact: true }).fill('2026-09-22T13:06:12');
    assert.equal(await form.getByLabel('失败原因', { exact: true }).count(), 0);
    await form.getByLabel('结果', { exact: true }).selectOption('失败');
    assert.equal(await form.getByLabel('失败原因', { exact: true }).getAttribute('required'), '');
    await form.getByLabel('失败原因', { exact: true }).fill('隔离测试原因');
    await form.getByRole('button', { name: '下一页', exact: true }).click();
    await form.getByRole('group', { name: 'E楼 202/A12', exact: true }).waitFor();
    assert.equal(await form.getByLabel('结果', { exact: true }).inputValue(), '成功');
    await form.getByLabel('排除状态', { exact: true }).check();
    await form.getByLabel('查找机柜记录更正', { exact: true }).fill('A11');
    await form.getByRole('group', { name: 'E楼 202/A11', exact: true }).waitFor();
    assert.equal(await form.getByLabel('失败原因', { exact: true }).inputValue(), '隔离测试原因');
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.screenshot({ path: path.join(output, `edit-${width}.png`), fullPage: true });
    await form.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.equal(writes.length, 1);
    const changed = writes[0].values[field.name];
    assert.equal(changed['row.0'].action, '上测试电');
    assert.equal(changed['row.0'].actual, '2026-09-22T13:06:12');
    assert.equal(changed['row.0'].failure_reason, '隔离测试原因');
    assert.equal(changed['row.1'].excluded, true);
    assert(!JSON.stringify(writes).includes('private-proof'));
    assert.deepEqual(errors, []);
    await context.close();
  }
  console.log('Assistant cabinet row edit native fields / pagination / desktop-mobile OK');
} finally { await browser.close(); }
