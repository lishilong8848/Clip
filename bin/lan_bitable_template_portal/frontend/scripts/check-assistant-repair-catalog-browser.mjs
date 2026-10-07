import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(scriptDir, '../../../..');
const python = path.join(root, 'bin/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');

// A field that is editable=False on the source record must NOT be rendered as an
// editable child; it is preserved verbatim in the plan value and on every submit.
const READONLY_NAME = '只读文本';
const READONLY_TEXT = '原始只读内容';

const synthesized = spawnSync(python, ['-c', `import sys,json,tempfile
sys.path.insert(0,'bin')
from pathlib import Path
from unittest.mock import Mock
from fastapi import FastAPI
from clipflow_backend.api_models import RepairFollowupRecordRequest
from lan_bitable_template_portal.lighthouse_ai import LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from test_lighthouse_agent_workflows import ACTOR, Store
tmp=tempfile.TemporaryDirectory()
store=Store(Path(tmp.name)/'s.sqlite3')
model=Mock(); model.settings.return_value={'configured':True,'enabled':True,'active_model_id':'default','models':[{'id':'default','name':'m','model':'f','configured':True}]}; model.profile.return_value={'id':'default','name':'m','model':'f'}
assistant=LighthouseAssistant(store, Mock(side_effect=AssertionError('x')), model=model)
files=LighthouseFiles(store)
app=FastAPI()
@app.put("/api/repair-management/followups/{record_id}")
async def uf(record_id: str, body: RepairFollowupRecordRequest): return {"ok":True,"data":{"record_id":record_id}}
@app.post("/api/repair-management/followups")
async def cf(body: RepairFollowupRecordRequest): return {"ok":True,"data":{"record_id":"c"}}
@app.get("/api/repair-management/cmdb-candidates")
async def devices(scope: str, q: str = "", limit: int = 80):
    rows=[{"record_id":"rec-device-a","name":"设备A","unique_id":"5.14.0","device_name":"设备A"},{"record_id":"rec-device-b","name":"设备B","unique_id":"5.14.1","device_name":"设备B"}]
    if q: rows=[r for r in rows if q in r["name"] or q in r["record_id"] or q in r["device_name"]]
    return {"ok":True,"data":{"records":rows,"has_more":False}}
catalog=PortalAPICatalog(app); agent=PortalAgent(assistant, catalog, files)
forig={"record_id":"rec-followup","record_version":"fv1","raw_fields":{"设备名称":"设备A","设备品牌":"品牌A","设备型号":"modelA-1","设备编号":"","维修进度":0.5,"${READONLY_NAME}":"${READONLY_TEXT}"},"cmdb_record_ids":[]}
fmetas=[{"field_name":"设备名称","field_type":1,"editable":True,"options":[]},{"field_name":"设备品牌","field_type":1,"editable":True,"options":[]},{"field_name":"设备型号","field_type":3,"editable":True,"options":["modelA-1","modelA-2"]},{"field_name":"设备编号","field_type":1,"editable":True,"options":[]},{"field_name":"维修进度","field_type":2,"editable":True,"options":[]},{"field_name":"${READONLY_NAME}","field_type":1,"editable":False,"options":[]}]
brand_model_options={"品牌A":["modelA-1","modelA-2"],"品牌B":["modelB-1","modelB-2"],"品牌C":["modelC-1","modelC-2"]}
device_brand_model_options={"设备A":{"品牌A":["modelA-1","modelA-2"]},"设备B":{"品牌B":["modelB-1","modelB-2"],"品牌C":["modelC-1","modelC-2"]}}
fqueries={"query_"+("b"*32):{"summary_record_id":"rec-parent","relation_mode":"record_id","fields":fmetas,"records":[forig],"brand_model_options":brand_model_options,"device_brand_model_options":device_brand_model_options}}
forig['raw_fields'].update({'维修方':'厂维','供应商名称':'旧供应商','供应商维修人员':'旧人员','更换备件名称':'旧轴承','更换备件数量':1})
fmetas.extend([{'field_name':name,'field_type':kind,'editable':True,'options':['我方','厂维'] if name=='维修方' else []}
              for name,kind in [('维修方',3),('供应商名称',1),('供应商维修人员',1),('更换备件名称',1),('更换备件数量',2)]])
fplan=agent.prepare(ACTOR,{"operations":[{"api_id":"PUT /api/repair-management/followups/{record_id}","path_params":{"record_id":"rec-followup"},"body":{"scope":"A","summary_record_id":"rec-parent","cmdb_record_ids":[]}}]},"opf",[],queries=fqueries)
# The browser receives the public API projection of the plan (what the server actually
# returns), never the raw internal prepare structure.
print(json.dumps(agent.public_plan(fplan, ACTOR), ensure_ascii=False))
`], { cwd: root, encoding: 'utf8', env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' } });
if (synthesized.status !== 0) throw new Error(synthesized.stderr || `python exited ${synthesized.status}`);
const followupPlan = JSON.parse(synthesized.stdout);

// The browser script must read the public API descriptor, no fabricated flags.
const fieldsControl = followupPlan.fields.find(f => f.path === 'fields');
assert.ok(fieldsControl.native_repair === true, 'fields control must be native_repair');
assert.equal(fieldsControl.native_repair_followup, true);
assert.ok(fieldsControl.repair_catalog?.brands && fieldsControl.repair_catalog?.devices, 'repair_catalog must be present');
assert.ok(fieldsControl.children.some(c => c.path === '设备型号' && c.allow_custom_select === true), '设备型号 must allow custom select');
assert.ok(fieldsControl.children.every(c => c.path !== '设备品牌' || c.allow_custom_select === false), '设备品牌 must NOT allow custom select');
assert.ok(fieldsControl.children.every(c => c.path !== READONLY_NAME), `readonly ${READONLY_NAME} must NOT be an editable child`);
assert.equal(fieldsControl.value['设备编号'], '', 'stored 设备编号 must be empty per fixture');
assert.equal(fieldsControl.value['设备品牌'], '品牌A');
assert.equal(fieldsControl.value['设备型号'], 'modelA-1');
assert.equal(fieldsControl.value[READONLY_NAME], READONLY_TEXT, `original readonly value must be present in the public plan value`);

const output = path.join(root, 'output/playwright/assistant-repair-catalog');
await mkdir(output, { recursive: true });
const base = 'http://127.0.0.1:19003';

const DEVICES = [
  { record_id: 'rec-device-a', name: '设备A', unique_id: '5.14.0', device_name: '设备A' },
  { record_id: 'rec-device-b', name: '设备B', unique_id: '5.14.1', device_name: '设备B' },
];
// Candidate device record ids and internal query refs must never leak into visible text;
// the record_id of the record being edited may legitimately appear in the operation preview.
const RAW_IDS = ['rec-device-a', 'rec-device-b', 'query_form_'];

function planField(form, label) {
  return form.locator('.plan-field').filter({ hasText: label }).first();
}
function repairControl(form, label) {
  return form.locator('.repair-field-control').filter({ hasText: label }).first();
}
function closeOpenMenus(page) {
  return page.keyboard.press('Escape');
}
// Real pointer selection of a teleported role=option button (no force). The product
// now raises the select menu via menuZIndex so the option is directly mouse-clickable.
async function chooseBrandOption(page, name) {
  await page.getByRole('option', { name, exact: true }).click();
}
async function noRawIds(page) {
  const visibleText = await page.evaluate(() => document.body.innerText || '');
  for (const id of RAW_IDS) assert.ok(!visibleText.includes(id), `raw id ${id} leaked into visible text`);
  const jsonTexts = await page.$$eval('textarea', els => els.map(el => el.value || ''));
  for (const id of RAW_IDS) for (const txt of jsonTexts) assert.ok(!txt.includes(id), `raw id ${id} leaked into JSON textarea`);
}
// Both the document and the nested .plan-form(s) must fit within their own client width.
async function assertNoHorizontalOverflow(page, width) {
  const metrics = await page.evaluate(() => ({ sw: document.documentElement.scrollWidth, cw: document.documentElement.clientWidth }));
  assert.ok(metrics.sw <= metrics.cw + 1, `document horizontal overflow at ${width}px: scrollWidth ${metrics.sw} > clientWidth ${metrics.cw}`);
  const formCount = await page.locator('.plan-form').count();
  assert.ok(formCount >= 1, `nested .plan-form must be present at ${width}px`);
  for (let i = 0; i < formCount; i += 1) {
    const bounds = await page.locator('.plan-form').nth(i).evaluate(el => ({ sw: el.scrollWidth, cw: el.clientWidth }));
    assert.ok(bounds.sw <= bounds.cw + 1, `nested .plan-form #${i} horizontal overflow at ${width}px: scrollWidth ${bounds.sw} > clientWidth ${bounds.cw}`);
  }
}
// Teleported .vnet-select-menu must be fully visible inside the viewport (not clipped).
async function assertDropdownsInViewport(page, width) {
  const menus = page.locator('.vnet-select-menu');
  const count = await menus.count();
  assert.ok(count >= 1, `an open vnet-select-menu is expected at ${width}px`);
  const vp = page.viewportSize();
  for (let i = 0; i < count; i += 1) {
    const box = await menus.nth(i).boundingBox();
    assert.ok(box && box.width > 0 && box.height > 0, `dropdown #${i} must be visible at ${width}px`);
    assert.ok(box.x >= -0.5 && box.x + box.width <= vp.width + 0.5,
      `dropdown #${i} horizontally out of bounds at ${width}px (x=${box.x} width=${box.width} vp=${vp.width})`);
    assert.ok(box.y >= -0.5 && box.y + box.height <= vp.height + 0.5,
      `dropdown #${i} vertically out of bounds at ${width}px (y=${box.y} height=${box.height} vp=${vp.height})`);
  }
}
function restoreEditing(basePlan, lastEditingFields, lastValues, version) {
  const fields = JSON.parse(JSON.stringify(lastEditingFields));
  for (const f of fields) if (lastValues && f.name in lastValues) f.value = JSON.parse(JSON.stringify(lastValues[f.name]));
  return { ...basePlan, status: 'needs_input', version, fields };
}

const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1100 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage();
    const errors = [];
    const saved = [];
    page.on('pageerror', error => errors.push(error.message));

    let editable = followupPlan;
    let display = followupPlan;
    let version = followupPlan.version;
    let lastEditingFields = null;
    let lastValues = null;
    let deviceSearchCount = 0;

    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: {
      ok: true,
      data: { conversation_id: 'repair-catalog-followup', configured: true, enabled: true, busy: false,
        turns: [{ operation_id: 'repair-catalog-op', question: '更新跟进记录', answer: '请确认跟进记录。', status: 'completed', plan: display }] } } }));

    await page.route('**/api/repair-management/cmdb-candidates?*', route => {
      const url = new URL(route.request().url());
      assert.equal(url.searchParams.get('scope'), 'A');
      const q = url.searchParams.get('q') || '';
      const rows = q ? DEVICES.filter(d => d.name.includes(q) || d.device_name.includes(q) || d.record_id.includes(q)) : DEVICES;
      return route.fulfill({ json: { ok: true, data: { records: rows, has_more: false } } });
    });

    await page.route('**/api/assistant/plans/*/options*', route => {
      const url = new URL(route.request().url());
      const field = url.searchParams.get('field');
      if (field.endsWith('.cmdb_record_ids')) {
        deviceSearchCount += 1;
        const selectedRaw = url.searchParams.get('selected') || '[]';
        const selected = JSON.parse(selectedRaw);
        assert.ok(Array.isArray(selected), 'cmdb option search must carry a selected array');
        editable = { ...editable, fields: editable.fields.map(f => f.path === 'cmdb_record_ids'
          ? { ...f, options: DEVICES.map(d => ({ value: d.record_id, label: d.name, device_name: d.device_name })) } : f) };
      }
      display = editable;
      return route.fulfill({ json: { ok: true, data: editable } });
    });

    await page.route(`**/api/assistant/plans/${followupPlan.id}`, route => {
      assert.equal(route.request().method(), 'PATCH');
      const body = route.request().postDataJSON();
      if (body.action === 'edit') {
        assert.equal(body.version, version, 'followup edit body.version must match');
        version += 1;
        editable = restoreEditing(followupPlan, lastEditingFields, lastValues, version);
        display = editable;
        return route.fulfill({ json: { ok: true, data: editable } });
      }
      assert.equal(body.version, version, 'followup submit body.version must match');
      const values = body.values || {};
      const fields = values['step0.fields'] || {};
      assert.equal(fields[READONLY_NAME], READONLY_TEXT, `readonly ${READONLY_NAME} must be preserved on submit`);
      saved.push(body);
      lastEditingFields = JSON.parse(JSON.stringify(editable.fields));
      lastValues = JSON.parse(JSON.stringify(values));
      version += 1;
      display = {
        ...editable,
        status: 'awaiting_confirmation',
        version,
        can_edit: true,
        fields: [],
        operations: [{
          api_id: 'PUT /api/repair-management/followups/{record_id}',
          name: '更新跟进记录',
          path_params: { record_id: 'rec-followup' },
          body: { scope: 'A', summary_record_id: 'rec-parent', cmdb_record_ids: values['step0.cmdb_record_ids'] || [], fields: values['step0.fields'] },
          selected_labels: { 'CMDB设备': '设备B', '楼栋': 'A' },
        }],
      };
      return route.fulfill({ json: { ok: true, data: display } });
    });

    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    let form = page.locator('.plan-form');
    await form.waitFor();

    const nameInput = () => repairControl(form, '设备名称').locator('input[data-repair-control]');
    const numberInput = () => repairControl(form, '设备编号').locator('input[data-repair-control]');
    const brandTrigger = () => repairControl(form, '设备品牌').locator('.vnet-select-trigger');
    const modelCombobox = () => repairControl(form, '设备型号').locator('.vnet-combobox-control input[role=combobox]');
    const progressInput = () => repairControl(form, '维修进度').locator('input[data-progress-number]');

    // 1) Initial stored fields: brandA/modelA, empty device_number, and the readonly
    //    original field stays as a value (not an editable input).
    assert.equal(await nameInput().inputValue(), '设备A', 'initial 设备名称 must be 设备A');
    assert.equal((await brandTrigger().innerText()).trim(), '品牌A', 'initial 设备品牌 must be 品牌A');
    assert.equal(await modelCombobox().inputValue(), 'modelA-1', 'initial 设备型号 must be modelA-1');
    assert.equal(await numberInput().inputValue(), '', 'initial 设备编号 must be empty');
    assert.equal(await progressInput().inputValue(), '50', 'initial 维修进度 0.5 maps to 50%');
    assert.equal(await page.locator(`.repair-field-control[data-field-name="${READONLY_NAME}"]`).count(), 0,
      `readonly ${READONLY_NAME} must NOT be rendered as an editable input`);
    assert.equal(await page.getByLabel(READONLY_NAME, { exact: true }).count(), 0,
      `readonly ${READONLY_NAME} must not expose any labelled editable control`);

    const spareToggle = () => form.getByLabel('是否涉及更换备件', { exact: true });
    assert.equal(await spareToggle().isChecked(), true, 'stored spare parts automatically expand');
    await spareToggle().uncheck();
    assert.equal(await form.getByLabel('更换备件名称', { exact: true }).count(), 0);
    await spareToggle().check();
    assert.equal(await form.getByLabel('更换备件名称', { exact: true }).inputValue(), '', 'turning off clears old spare names');
    assert.equal(await form.getByLabel('更换备件数量', { exact: true }).inputValue(), '', 'turning off clears old spare count');
    await form.getByLabel('更换备件名称', { exact: true }).fill('新轴承');
    await form.getByLabel('更换备件数量', { exact: true }).fill('2');
    await form.getByLabel('维修方', { exact: true }).selectOption('我方');
    for (const name of ['供应商名称', '供应商维修人员']) {
      assert.equal(await form.getByLabel(name, { exact: true }).inputValue(), '', 'internal repair clears supplier fields');
      assert.equal(await form.getByLabel(name, { exact: true }).isDisabled(), true);
    }
    await form.getByLabel('维修方', { exact: true }).selectOption('厂维');
    assert.equal(await form.getByLabel('供应商名称', { exact: true }).isDisabled(), false);
    assert.equal(await form.getByLabel('供应商名称', { exact: true }).inputValue(), '', 'old supplier must not return');
    await form.getByLabel('维修方', { exact: true }).selectOption('我方');

    // Initial brand menu reflects the currently selected 品牌A and only 设备A's brand.
    await brandTrigger().click();
    const brandAOption = page.getByRole('option', { name: '品牌A', exact: true });
    await brandAOption.waitFor();
    assert.equal(await brandAOption.count(), 1, '品牌A must be offered for 设备A');
    assert.equal(await brandAOption.getAttribute('aria-selected'), 'true', 'currently selected 品牌A must be marked selected');
    assert.equal(await page.getByRole('option', { name: '品牌B', exact: true }).count(), 0, '品牌B must NOT be offered for 设备A');
    await assertDropdownsInViewport(page, width);
    await closeOpenMenus(page);

    // Initial model combobox shows the currently selected model and only brandA models.
    await modelCombobox().focus();
    const modelA1Option = page.getByRole('option', { name: 'modelA-1', exact: true });
    await modelA1Option.waitFor();
    assert.equal(await modelA1Option.count(), 1, 'current modelA-1 must be offered');
    assert.equal(await modelA1Option.getAttribute('aria-selected'), 'true', 'currently selected modelA-1 must be marked selected');
    assert.equal(await page.getByRole('option', { name: 'modelA-2', exact: true }).count(), 1, 'modelA-2 must be offered');
    assert.equal(await page.getByRole('option', { name: 'modelB-1', exact: true }).count(), 0, 'modelB-1 must NOT be offered initially');
    await assertDropdownsInViewport(page, width);
    await closeOpenMenus(page);

    // 2) Choose deviceB by typing 设备名称 -> incompatible brand/model cleared, number syncs.
    await nameInput().fill('设备B');
    assert.equal(await numberInput().inputValue(), '设备B', '设备编号 must sync to device name when it was empty');
    assert.equal((await brandTrigger().innerText()).trim(), '请选择设备品牌', 'incompatible 设备品牌 must be cleared');
    assert.equal(await modelCombobox().inputValue(), '', '设备型号 must be cleared with incompatible brand');
    assert.equal(await modelCombobox().isDisabled(), true, 'select brand before editing model');

    // 3) Compatible brand via VnetSelect, mouse-clicked -> only deviceB brands offered (no 品牌A).
    await brandTrigger().click();
    await page.getByRole('option', { name: '品牌B', exact: true }).waitFor();
    assert.equal(await page.getByRole('option', { name: '品牌A', exact: true }).count(), 0, '设备A only brand must NOT be offered for 设备B');
    assert.equal(await page.getByRole('option', { name: '品牌B', exact: true }).count(), 1, '品牌B must be offered');
    assert.equal(await page.getByRole('option', { name: '品牌C', exact: true }).count(), 1, '品牌C must be offered');
    await assertDropdownsInViewport(page, width);
    await chooseBrandOption(page, '品牌B');
    assert.equal((await brandTrigger().innerText()).trim(), '品牌B', 'mouse-clicked compatible brand must be 品牌B');

    // 4) Model combobox: only the selected brand's model options; valid model selected by mouse.
    await modelCombobox().focus();
    await page.getByRole('option', { name: 'modelB-1', exact: true }).waitFor();
    assert.equal(await page.getByRole('option', { name: 'modelB-2', exact: true }).count(), 1, 'only modelB options from brandB');
    assert.equal(await page.getByRole('option', { name: 'modelA-1', exact: true }).count(), 0, 'brandA model must NOT be offered');
    assert.equal(await page.getByRole('option', { name: 'modelC-1', exact: true }).count(), 0, 'brandC model must NOT be offered');
    await assertDropdownsInViewport(page, width);
    await page.getByRole('option', { name: 'modelB-1', exact: true }).click();
    assert.equal(await modelCombobox().inputValue(), 'modelB-1', 'mouse-clicked valid modelB-1 accepted');

    // Supplementary keyboard check on the same model menu must also work.
    // The native VnetSelect selectOption closes the menu and restores focus, so a bare
    // focus() does NOT fire a new focus event to re-open it. Press ArrowDown to open the
    // closed menu, then navigate to modelB-2 and confirm with Enter.
    await modelCombobox().focus();
    await page.keyboard.press('ArrowDown');
    await page.getByRole('option', { name: 'modelB-2', exact: true }).waitFor();
    await page.keyboard.press('ArrowDown');
    await page.keyboard.press('Enter');
    assert.equal(await modelCombobox().inputValue(), 'modelB-2', 'keyboard selection still works on the model menu');

    // 5) Custom model accepted (native behavior for 设备型号).
    await modelCombobox().fill('模型自定义-算例');
    assert.equal(await modelCombobox().inputValue(), '模型自定义-算例', 'custom model must be accepted');
    // Typing a custom model opens the combobox menu; close it before switching brand so the
    // open model dropdown does not intercept the subsequent brand option click.
    await closeOpenMenus(page);

    // 6) Brand switch clears incompatible model (品牌B -> 品牌C has no modelB).
    await brandTrigger().click();
    await chooseBrandOption(page, '品牌C');
    assert.equal((await brandTrigger().innerText()).trim(), '品牌C', 'brand switched to 品牌C');
    assert.equal(await modelCombobox().inputValue(), '', 'switching to incompatible brand must clear model');
    // restore a valid combination for later submit: brand 品牌B + modelB-2
    await brandTrigger().click();
    await chooseBrandOption(page, '品牌B');
    await modelCombobox().fill('modelB-2');
    assert.equal(await modelCombobox().inputValue(), 'modelB-2');

    // 7) CMDB selection calls the same dependent clear (select 设备A while brand=品牌B is
    //    incompatible with 设备A's brand set).
    const cmdbField = planField(form, 'CMDB设备');
    await cmdbField.getByRole('button', { name: '查找', exact: true }).click();
    await cmdbField.getByText('设备A', { exact: true }).waitFor();
    await cmdbField.locator('label', { hasText: '设备A' }).locator('input[type=checkbox]').check();
    assert.equal(await nameInput().inputValue(), '设备A', 'CMDB autofill must set 设备名称=设备A');
    assert.equal((await brandTrigger().innerText()).trim(), '请选择设备品牌', 'CMDB select of incompatible device clears brand');
    assert.equal(await modelCombobox().inputValue(), '', 'CMDB select of incompatible device clears model');

    // 8) Clearing selection only touches the original editable fields (name/number clear,
    //    brand/model and other edited data are not wiped while device is blank).
    await cmdbField.getByRole('button', { name: '清空CMDB设备', exact: true }).click();
    await cmdbField.locator('label', { hasText: '设备A' }).first().waitFor();
    assert.equal(await nameInput().inputValue(), '', 'clearing CMDB must clear 设备名称');
    assert.equal(await numberInput().inputValue(), '', 'clearing CMDB must clear 设备编号');
    assert.equal((await brandTrigger().innerText()).trim(), '请选择设备品牌', 'brand stays cleared (was already cleared)');
    assert.equal(await modelCombobox().inputValue(), '', 'model stays cleared');
    assert.equal(await progressInput().inputValue(), '50', 'unrelated 维修进度 must be untouched by dependent clear');

    // 9) Final valid choice for submit: 设备B + brandB + custom model. Keep progress edited.
    await nameInput().fill('设备B');
    await brandTrigger().click();
    await chooseBrandOption(page, '品牌B');
    await modelCombobox().fill('模型自定义-最后');
    await progressInput().fill('80');
    assert.equal(await progressInput().inputValue(), '80', 'edited progress retained');

    await noRawIds(page);

    // 10) Submit -> verified values, readonly original field preserved.
    await form.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.equal(saved.length, 1, 'followup submit must happen once');
    const submittedFields = saved[0].values['step0.fields'];
    assert.equal(submittedFields['设备名称'], '设备B');
    assert.equal(submittedFields['设备品牌'], '品牌B');
    assert.equal(submittedFields['设备型号'], '模型自定义-最后');
    assert.equal(submittedFields['维修方'], '我方');
    assert.equal(submittedFields['供应商名称'], '');
    assert.equal(submittedFields['供应商维修人员'], '');
    assert.equal(submittedFields['更换备件名称'], '新轴承');
    assert.equal(String(submittedFields['更换备件数量']), '2');
    assert.equal(submittedFields[READONLY_NAME], READONLY_TEXT, `experiment readonly ${READONLY_NAME} must be submitted unchanged`);

    // 11) Return-edit retains edits.
    await page.getByRole('button', { name: '返回修改', exact: true }).click();
    form = page.locator('.plan-form');
    await form.waitFor();
    assert.equal(await nameInput().inputValue(), '设备B', '设备名称 retained after return-edit');
    assert.equal((await brandTrigger().innerText()).trim(), '品牌B', '设备品牌 retained after return-edit');
    assert.equal(await modelCombobox().inputValue(), '模型自定义-最后', '设备型号 retained after return-edit');
    assert.equal(await progressInput().inputValue(), '80', '维修进度 retained after return-edit');

    // 12) Reload keeps the returned edits.
    await page.reload();
    form = page.locator('.plan-form');
    await form.waitFor();
    assert.equal(await nameInput().inputValue(), '设备B', '设备名称 retained after reload');
    assert.equal((await brandTrigger().innerText()).trim(), '品牌B', '设备品牌 retained after reload');
    assert.equal(await modelCombobox().inputValue(), '模型自定义-最后', '设备型号 retained after reload');
    assert.equal(await progressInput().inputValue(), '80', '维修进度 retained after reload');
    assert.equal(await spareToggle().isChecked(), true, 'spare controls restore after reload');
    assert.equal(await form.getByLabel('更换备件名称', { exact: true }).inputValue(), '新轴承');
    assert.equal(await form.getByLabel('供应商名称', { exact: true }).isDisabled(), true);

    await assertNoHorizontalOverflow(page, width);
    assert.deepEqual(errors, []);
    await page.screenshot({ path: path.join(output, `repair-catalog-${width}.png`), fullPage: true });
    await context.close();
    console.log(`RepairCatalog ${width}px: initial-selected/typing-deviceB-clears/mouse-brand(/keyboard-supplement)/mouse-model-only-brandB/custom-model/brand-switch-clears/cmdb-dependent-clear/clear-only-editable/readonly-preserved/submit/return-edit/reload/no-pageerror/no-raw-id/nested-plan-form+dropdown-bounds OK`);
  }
} finally {
  await browser.close();
}
