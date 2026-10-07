import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const python = path.join(root, 'bin/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
const fixture = JSON.parse(execFileSync(python, ['-c', `import sys,json
sys.path.insert(0,'bin')
from test_lighthouse_mop_workflows import _build_workbook_bytes
from lan_bitable_template_portal.portal_service import MaintenancePortalService
from lan_bitable_template_portal.lighthouse_api import _mop_frontend_fields
preview=MaintenancePortalService()._parse_xlsx_preview(_build_workbook_bytes())
control=_mop_frontend_fields(preview)
value=control.pop('_initial_form')
for block in control['children']:
 for child in block.get('children',[]):
  if child['path'] in ('implementer','auditor'):
   child['options']=[{'value':'ref-'+child['path'],'label':'王实施 · P001' if child['path']=='implementer' else '李审核 · P002'}]
print(json.dumps({'control':control,'value':value},ensure_ascii=False))
`], { cwd: root, env: { ...process.env, PYTHONIOENCODING: 'utf-8' }, encoding: 'utf8' }));
const output = path.join(root, 'output/playwright/assistant-mop');
await mkdir(output, { recursive: true });
const base = 'http://127.0.0.1:19003';
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), saved = [], errors = [];
    page.on('pageerror', error => errors.push(error.message));
    let plan = { id: 'mop-form', status: 'needs_input', title: '填写维护单', version: 1, fields: [{ ...fixture.control, name: 'mop', path: '', label: '维护单填写（当前工作表）', value: structuredClone(fixture.value) }], operations: [], results: [] };
    const state = () => ({ ok: true, data: { conversation_id: 'mop-fixture', configured: true, enabled: true, busy: false, turns: [{ operation_id: 'mop-fixture', question: '填写并下载维护单', answer: '请填写', status: 'completed', plan }] } });
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: state() }));
    await page.route('**/api/assistant/plans/mop-form', route => {
      saved.push(route.request().postDataJSON());
      plan = { ...plan, fields: [], status: 'awaiting_confirmation', version: 2 };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    const form = page.locator('.plan-form');
    await form.waitFor();
    assert.equal(await form.getByLabel('工作表', { exact: true }).inputValue(), '维护单');
    assert.equal(await form.getByLabel('工作表', { exact: true }).locator('option').filter({ hasText: '封面' }).count(), 0);
    await form.getByLabel('维护开始时间 · C3', { exact: true }).fill('2026-10-02T10:30');
    await form.getByLabel('维护完成时间 · A5', { exact: true }).fill('2026-10-02T16:00');
    await form.getByLabel('A4 · 维护完成情况：□正常 □异常', { exact: true }).selectOption({ label: '正常' });
    await form.getByRole('group', { name: '维护实施人', exact: true }).getByRole('checkbox', { name: '王实施 · P001', exact: true }).check();
    await form.getByRole('group', { name: '维护审核人', exact: true }).getByRole('checkbox', { name: '李审核 · P002', exact: true }).check();
    const edits = form.getByRole('group', { name: '单元格编辑', exact: true });
    await edits.getByRole('button', { name: '添加单元格编辑', exact: true }).click();
    await edits.getByLabel('行', { exact: true }).fill('6');
    await edits.getByLabel('列', { exact: true }).selectOption('C');
    await edits.getByLabel('单元格内容', { exact: true }).fill('已按时完成维护');
    assert.equal(await form.getByLabel('local_file_path', { exact: true }).count(), 0);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
    await page.screenshot({ path: path.join(output, `mop-${width}.png`), fullPage: true });
    await form.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.equal(saved.length, 1);
    const values = saved[0].values.mop;
    assert.equal(values.sheet_name, '维护单');
    assert.equal(values.sheet_0.fields.field_0, '2026-10-02T10:30');
    assert.equal(values.sheet_0.fields.field_1, '2026-10-02T16:00');
    assert.deepEqual(values.sheet_0.implementer, ['ref-implementer']);
    assert.deepEqual(values.sheet_0.auditor, ['ref-auditor']);
    assert.deepEqual(values.sheet_0.cell_edits, [{ row: 6, column: 'C', value: '已按时完成维护' }]);
    assert.deepEqual(errors, []);
    await context.close();
  }
  console.log('Assistant native MOP fields desktop/mobile OK');
} finally { await browser.close(); }
