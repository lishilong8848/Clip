import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const python = path.join(root, 'bin/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
const synthesized = spawnSync(python, ['-c', `import sys,json,tempfile
sys.path.insert(0,'bin')
from pathlib import Path
from unittest.mock import Mock
from fastapi import FastAPI
from clipflow_backend.api_models import RepairFollowupRecordRequest, RepairManagementRecordRequest
from lan_bitable_template_portal.lighthouse_ai import LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.portal_service import REPAIR_MANAGEMENT_TABLE_ID
from test_lighthouse_agent_workflows import ACTOR, Store
tmp=tempfile.TemporaryDirectory()
store=Store(Path(tmp.name)/'s.sqlite3')
model=Mock(); model.settings.return_value={'configured':True,'enabled':True,'active_model_id':'default','models':[{'id':'default','name':'m','model':'f','configured':True}]}; model.profile.return_value={'id':'default','name':'m','model':'f'}
assistant=LighthouseAssistant(store, Mock(side_effect=AssertionError('x')), model=model)
files=LighthouseFiles(store)
app=FastAPI()
@app.put("/api/repair-management/records/{record_id}")
async def up(record_id: str, body: RepairManagementRecordRequest): return {"ok":True,"data":{"record_id":record_id}}
@app.post("/api/repair-management/records")
async def cp(body: RepairManagementRecordRequest): return {"ok":True,"data":{"record_id":"c"}}
@app.put("/api/repair-management/followups/{record_id}")
async def uf(record_id: str, body: RepairFollowupRecordRequest): return {"ok":True,"data":{"record_id":record_id}}
@app.post("/api/repair-management/followups")
async def cf(body: RepairFollowupRecordRequest): return {"ok":True,"data":{"record_id":"c"}}
@app.get("/api/repair-management/cmdb-candidates")
async def devices(scope: str, q: str = "", limit: int = 80):
    rows=[{"record_id":"rec-device-0","name":"柴油发电机","unique_id":"5.14.0","device_name":"柴油发电机"},{"record_id":"rec-device-1","name":"UPS主机","unique_id":"5.14.1","device_name":"UPS主机"},{"record_id":"rec-device-old","name":"旧设备","unique_id":"5.14.9","device_name":"旧设备"}]
    if q: rows=[r for r in rows if q in r["name"] or q in r["record_id"] or q in r["device_name"]]
    return {"ok":True,"data":{"records":rows,"has_more":False}}
@app.get("/api/repair-management/event-candidates")
async def events(scope: str, q: str = "", limit: int = 80):
    rows=[{"record_id":"rec-event","name":"事件A","display_fields":{"事件简述":"事件A"}},{"record_id":"rec-event-2","name":"事件B","display_fields":{"事件简述":"事件B"}}]
    if q: rows=[r for r in rows if q in str(r.get("name")) or q in json.dumps(r.get("display_fields",{}),ensure_ascii=False)]
    return {"ok":True,"data":{"records":rows,"has_more":False}}
@app.get("/api/repair-management/repair-candidates")
async def repairs(scope: str, q: str = "", limit: int = 80, event_record_id: str = ""):
    rows=[{"record_id":"rec-repair","name":"检修通告A","display_fields":{"检修通告名称":"检修A"}},{"record_id":"rec-repair-2","name":"检修通告B","display_fields":{"检修通告名称":"检修B"}}]
    if event_record_id and event_record_id not in {"rec-event","rec-event-2"}: rows=[]
    if q: rows=[r for r in rows if q in str(r.get("name")) or q in json.dumps(r.get("display_fields",{}),ensure_ascii=False)]
    return {"ok":True,"data":{"records":rows,"has_more":False}}
catalog=PortalAPICatalog(app); agent=PortalAgent(assistant, catalog, files)
porig={"record_id":"rec-project","record_version":"v1","building_codes":["A"],"raw_fields":{"故障维修原因":[{"text":"轴承磨损"}],"故障发生时间":"2026-01-05 10:30","所属专业":"电气","设备名称":"旧设备","设备编号":"OLD-1"},"source_event_id":"rec-event","source_repair_ids":["rec-repair"]}
pmetas=[{"field_name":"故障维修原因","field_type":1,"editable":True,"options":[]},{"field_name":"故障发生时间","field_type":5,"editable":True,"options":[]},{"field_name":"所属专业","field_type":3,"editable":True,"options":["电气","暖通"]},{"field_name":"设备名称","field_type":1,"editable":False,"editable_without_repair_link":True,"options":[]},{"field_name":"设备编号","field_type":1,"editable":True,"options":[]}]
pqueries={"query_"+"a"*32:{"table_id":REPAIR_MANAGEMENT_TABLE_ID,"records":[porig],"fields":pmetas}}
pplan=agent.prepare(ACTOR,{"operations":[{"api_id":"PUT /api/repair-management/records/{record_id}","path_params":{"record_id":"rec-project"},"body":{"scope":"A"}}]},"opp",[],queries=pqueries)
forig={"record_id":"rec-followup","record_version":"fv1","raw_fields":{"维修进度":0.5,"设备名称":"旧设备","设备编号":"OLD-1"},"cmdb_record_ids":["rec-device-old"]}
fmetas=[{"field_name":"维修进度","field_type":2,"editable":True,"options":[]},{"field_name":"设备名称","field_type":1,"editable":True,"options":[]},{"field_name":"设备编号","field_type":1,"editable":True,"options":[]}]
fqueries={"query_"+"b"*32:{"summary_record_id":"rec-parent","relation_mode":"record_id","fields":fmetas,"records":[forig]}}
fplan=agent.prepare(ACTOR,{"operations":[{"api_id":"PUT /api/repair-management/followups/{record_id}","path_params":{"record_id":"rec-followup"},"body":{"scope":"A","summary_record_id":"rec-parent","cmdb_record_ids":["rec-device-old"]}}]},"opf",[],queries=fqueries)
print(json.dumps({"project":agent.public_plan(pplan, ACTOR),"followup":agent.public_plan(fplan, ACTOR)}, ensure_ascii=False))
`], { cwd: root, encoding: 'utf8', env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' } });
if (synthesized.status !== 0) throw new Error(synthesized.stderr || `python exited ${synthesized.status}`);
const plans = JSON.parse(synthesized.stdout);
const projectPlan = plans.project;
const followupPlan = plans.followup;

const projectFields = projectPlan.fields.find(f => f.path === 'fields');
assert.ok(projectFields.unlinked_children, 'project fields control must expose unlinked_children');
assert.equal(projectFields.children.map(c => c.path).includes('设备名称'), false, 'linked view must NOT show the unlinked-only field');
assert.equal(projectFields.unlinked_children.map(c => c.path).includes('设备名称'), true, 'unlinked view must expose 设备名称');
assert.equal(projectFields.native_repair_prefill, true, 'project fields control must expose native_repair_prefill');

const output = path.join(root, 'output/playwright/assistant-repair-relations');
await mkdir(output, { recursive: true });
const base = 'http://127.0.0.1:19003';
const browser = await chromium.launch({ headless: true });

const EVENTS = [
  { record_id: 'rec-event', name: '事件A', display_fields: { 事件简述: '事件A' } },
  { record_id: 'rec-event-2', name: '事件B', display_fields: { 事件简述: '事件B' } },
];
const REPAIRS = {
  'rec-event': [{ record_id: 'rec-repair', name: '检修通告A', display_fields: { 检修通告名称: '检修A' } }],
  'rec-event-2': [{ record_id: 'rec-repair-2', name: '检修通告B', display_fields: { 检修通告名称: '检修B' } }],
};
const DEVICES = [
  { record_id: 'rec-device-old', name: '旧设备', unique_id: '5.14.9', device_name: '旧设备' },
  { record_id: 'rec-device-0', name: '柴油发电机', unique_id: '5.14.0', device_name: '柴油发电机' },
  { record_id: 'rec-device-1', name: 'UPS主机', unique_id: '5.14.1', device_name: 'UPS主机' },
];
const RAW_IDS = ['rec-event', 'rec-event-2', 'rec-repair', 'rec-repair-2', 'rec-device-old', 'rec-device-0', 'rec-device-1'];
// Numeric timestamp that renders predictably to 2025-04-30T16:00 in Asia/Shanghai.
const NATIVE_DATE_MS = 1746000000000;
const NATIVE_DATE_TEXT = '2025-04-30T16:00';

function eventOptions() {
  return [{ value: '__empty__', label: '不关联' }, ...EVENTS.map(e => ({ value: e.record_id, label: e.name }))];
}
function withFieldOptions(plan, path, options) {
  return { ...plan, fields: plan.fields.map(f => f.path === path ? { ...f, options: JSON.parse(JSON.stringify(options)) } : f) };
}
function planField(form, label) {
  return form.locator('.plan-field').filter({ hasText: label }).first();
}
async function noRawIds(page) {
  const visibleText = await page.evaluate(() => document.body.innerText || '');
  for (const id of RAW_IDS) assert.ok(!visibleText.includes(id), `raw id ${id} leaked into visible text`);
  const jsonTexts = await page.$$eval('textarea', els => els.map(el => el.value || ''));
  for (const id of RAW_IDS) for (const txt of jsonTexts) assert.ok(!txt.includes(id), `raw id ${id} leaked into JSON textarea`);
}
// Rebuild the editable plan on return-edit: keep last loaded options but apply the
// user-supplied values (which carry selections + manual overrides).
function restoreEditing(basePlan, lastEditingFields, lastValues, version) {
  const fields = JSON.parse(JSON.stringify(lastEditingFields));
  for (const f of fields) if (lastValues && f.name in lastValues) f.value = JSON.parse(JSON.stringify(lastValues[f.name]));
  return { ...basePlan, status: 'needs_input', version, fields };
}
// Poll a repair-field input until it settles to the expected value (no forced value injection).
async function expectRepairValue(page, form, label, expected, what = label) {
  const input = form.locator('.repair-field-control').filter({ hasText: label }).locator('[data-repair-control]').first();
  await input.waitFor();
  for (let attempt = 0; attempt < 60; attempt++) {
    if ((await input.inputValue()) === expected) return;
    await page.waitForTimeout(100);
  }
  assert.equal(await input.inputValue(), expected, `${what} should settle to ${JSON.stringify(expected)}`);
}
// Poll the unlinked-only 设备名称 until it reaches the expected count (visible/hidden).
async function expectRepairControlCount(form, label, expected) {
  for (let attempt = 0; attempt < 60; attempt++) {
    const count = await form.locator('.repair-field-control').filter({ hasText: label }).count();
    if (count === expected) return;
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  assert.equal(await form.locator('.repair-field-control').filter({ hasText: label }).count(), expected, `${label} control count should settle to ${expected}`);
}
async function expectNoHorizontalOverflow(page) {
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth + 1);
  assert.equal(overflow, false, 'page must not have horizontal overflow');
}

try {
  for (const width of [1440, 390]) {
    // =========================================================== PROJECT
    {
      const context = await browser.newContext({ viewport: { width, height: 1100 }, timezoneId: 'Asia/Shanghai' });
      assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
      const page = await context.newPage();
      const errors = [];
      const saved = [];
      page.on('pageerror', error => errors.push(error.message));

      let editable = projectPlan; // current needs_input plan (values + options)
      let display = projectPlan;  // what the conversation returns
      let version = projectPlan.version;
      let lastEditingFields = null;
      let lastValues = null;
      let prefillCalls = 0;
      let nativeWrites = 0;

      const conversation = () => ({
        ok: true,
        data: { conversation_id: 'repair-relations-project', configured: true, enabled: true, busy: false,
          turns: [{ operation_id: 'repair-relations-op', question: '更新维修项目', answer: '请确认维修项目信息。', status: 'completed', plan: display }] },
      });

      await page.route('**/api/assistant/conversation', route => route.fulfill({ json: conversation() }));

      await page.route('**/api/repair-management/event-candidates?*', route => {
        const url = new URL(route.request().url());
        assert.equal(url.searchParams.get('scope'), 'A');
        return route.fulfill({ json: { ok: true, data: { records: EVENTS, has_more: false } } });
      });
      await page.route('**/api/repair-management/repair-candidates?*', route => {
        const url = new URL(route.request().url());
        assert.equal(url.searchParams.get('scope'), 'A');
        const eventId = url.searchParams.get('event_record_id') || '';
        return route.fulfill({ json: { ok: true, data: { records: REPAIRS[eventId] || [], has_more: false } } });
      });

      await page.route('**/api/assistant/plans/*/options*', route => {
        const url = new URL(route.request().url());
        const field = url.searchParams.get('field');
        const sourceEvent = url.searchParams.get('source_event_id') || '';
        if (field.endsWith('.source_event_id')) {
          editable = withFieldOptions(editable, 'source_event_id', eventOptions());
        } else if (field.endsWith('.source_repair_ids')) {
          assert.equal(sourceEvent, 'rec-event-2', `repair option search must carry the NEW event id, got ${sourceEvent}`);
          editable = withFieldOptions(editable, 'source_repair_ids', (REPAIRS[sourceEvent] || []).map(r => ({ value: r.record_id, label: r.name })));
        }
        display = editable;
        return route.fulfill({ json: { ok: true, data: editable } });
      });

      // Guard against any real native business write during the interaction phase.
      await page.route('**/api/repair-management/records', route => { nativeWrites += 1; return route.fulfill({ status: 500, json: { ok: false, error: 'unexpected native write' } }); });
      await page.route('**/api/repair-management/records/*', route => { nativeWrites += 1; return route.fulfill({ status: 500, json: { ok: false, error: 'unexpected native write' } }); });
      await page.route('**/api/repair-management/followups', route => { nativeWrites += 1; return route.fulfill({ status: 500, json: { ok: false, error: 'unexpected native write' } }); });
      await page.route('**/api/repair-management/followups/*', route => { nativeWrites += 1; return route.fulfill({ status: 500, json: { ok: false, error: 'unexpected native write' } }); });

      // Native prefill: first call fails (after a bounded delay so loading is observable),
      // retry (2nd call) is deferred so loading + disabled submit can be asserted, and
      // later calls succeed. The event-only / linked responses force the source-derived
      // fields (including 所属专业 via source_field_names) because the event differs from
      // the original operation body (A -> B).
      await page.route('**/api/assistant/plans/*/repair-prefill', async route => {
        assert.equal(route.request().method(), 'POST');
        const body = route.request().postDataJSON();
        assert.equal(body.version, version, 'prefill body.version must match current plan version');
        assert.equal(body.operation_index, 0, 'prefill body.operation_index must be 0');
        assert.equal(body.scope, 'A', 'prefill body.scope must be A');
        const event = body.source_event_id || '';
        const repairs = body.source_repair_ids || [];
        prefillCalls += 1;
        if (prefillCalls === 1) {
          await new Promise(resolve => setTimeout(resolve, 250));
          return route.fulfill({ status: 502, json: { ok: false, error: '关联资料读取未完成，请稍后重试。' } });
        }
        const respond = () => {
          if (!event && !repairs.length) {
            // Clearing the event removes automatic values; nothing to fill back.
            return { version, fields: {}, controlled_fields: [], source_field_names: [], warnings: [], skip: false };
          }
          const fields = { '故障发生时间': NATIVE_DATE_MS, '故障维修原因': '轴承故障', '所属专业': '电气' };
          const controlled_fields = ['故障发生时间', '故障维修原因'];
          const source_field_names = ['故障发生时间', '所属专业'];
          return { version, fields, controlled_fields, source_field_names, warnings: [], skip: false };
        };
        if (prefillCalls === 2) await new Promise(resolve => setTimeout(resolve, 500));
        return route.fulfill({ json: { ok: true, data: respond() } });
      });

      await page.route(`**/api/assistant/plans/${projectPlan.id}`, route => {
        assert.equal(route.request().method(), 'PATCH');
        const body = route.request().postDataJSON();
        if (body.action === 'edit') {
          assert.equal(body.version, version, 'edit body.version must match current plan.version');
          version += 1;
          editable = restoreEditing(projectPlan, lastEditingFields, lastValues, version);
          display = editable;
          return route.fulfill({ json: { ok: true, data: editable } });
        }
        assert.equal(body.version, version, 'submit body.version must match current plan.version');
        const values = body.values || {};
        assert.equal(values['step0.source_event_id'], 'rec-event-2', 'project submit must carry NEW source_event_id');
        assert.deepEqual(values['step0.source_repair_ids'], ['rec-repair-2'], 'project submit must carry the NEW repair id');
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
            api_id: 'PUT /api/repair-management/records/{record_id}',
            name: '更新维修项目',
            path_params: { record_id: 'rec-project' },
            body: { scope: 'A', source_event_id: 'rec-event-2', source_repair_ids: ['rec-repair-2'] },
            selected_labels: { '关联事件': '事件B', '关联检修通告': '检修通告B', '楼栋': 'A' },
          }],
        };
        return route.fulfill({ json: { ok: true, data: display } });
      });

      await page.goto(base);
      await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
      let form = page.locator('.plan-form');
      await form.waitFor();

      // 1) Existing event + repair selected; unlinked-only field hidden while linked.
      const eventField = planField(form, '关联事件');
      assert.equal(await eventField.locator('select').inputValue(), 'rec-event', 'original event must be selected');
      const repairField = planField(form, '关联检修通告');
      assert.equal(await repairField.locator('input[type=checkbox]').count(), 1, 'one original repair option expected');
      assert.equal(await repairField.locator('input[type=checkbox]').first().isChecked(), true, 'original repair must be checked');
      assert.equal(await form.locator('.repair-field-control').filter({ hasText: '设备名称' }).count(), 0, 'unlinked-only field hidden in linked view');

      // 2) Manual edits BEFORE switching the event: manual device number + source-derived 所属专业.
      await expectRepairValue(page, form, '设备编号', 'OLD-1', 'original device number');
      await form.locator('.repair-field-control').filter({ hasText: '设备编号' }).locator('input[data-repair-control]').fill('手工-1');
      const specialtySelect = () => form.locator('.lh-sf-c').filter({ hasText: '所属专业' }).locator('select.lh-sf-input').first();
      await specialtySelect().waitFor();
      await specialtySelect().selectOption('暖通');

      // 3) Load event options, change event A -> B. The FIRST native prefill FAILS.
      await eventField.getByRole('button', { name: '查找', exact: true }).click();
      await eventField.locator('select').selectOption('rec-event-2');
      assert.equal(await repairField.locator('input[type=checkbox]').count(), 0, 'changing event must clear the repair selection');
      const prefillState = form.locator('.repair-prefill-state');
      await prefillState.waitFor();
      await form.getByRole('button', { name: '重新读取关联资料', exact: true }).waitFor();
      // Draft (manual edits) + selected event preserved despite the failed prefill.
      assert.equal(await eventField.locator('select').inputValue(), 'rec-event-2', 'selected event must survive the failed prefill');
      assert.equal(await form.locator('.repair-field-control').filter({ hasText: '设备编号' }).locator('input[data-repair-control]').inputValue(), '手工-1', 'manual device number must survive the failed prefill');
      assert.equal(await specialtySelect().inputValue(), '暖通', 'manual 所属专业 edit must survive the failed prefill');
      assert.equal(await form.getByRole('button', { name: '补充并继续', exact: true }).isDisabled(), true, 'Continue must be blocked while the prefill is in error');
      assert.equal(prefillCalls, 1, 'exactly one (failed) prefill call so far');

      // 4) Retry succeeds. Native cause+date auto fill (deterministic Asia/Shanghai, numeric
      //    timestamp); the manual unrelated device number is preserved; the changed event forces
      //    source_field_names to override the (now dirty) source-derived 所属专业.
      const continueBtn = form.getByRole('button', { name: '补充并继续', exact: true });
      await form.getByRole('button', { name: '重新读取关联资料', exact: true }).click();
      await page.getByText('正在读取关联资料…', { exact: true }).waitFor({ timeout: 2000 });
      assert.equal(await continueBtn.isDisabled(), true, 'Continue must be disabled while the prefill is loading');
      await expectRepairValue(page, form, '故障维修原因', '轴承故障', 'auto-filled cause');
      await expectRepairValue(page, form, '故障发生时间', NATIVE_DATE_TEXT, 'auto-filled date (Asia/Shanghai)');
      assert.equal(await continueBtn.isDisabled(), false, 'Continue must be enabled after the prefill succeeds');
      assert.equal(await specialtySelect().inputValue(), '电气', 'source_field_names must force 所属专业 back to the event-derived value (body A vs event B)');
      // Unlinked view: the unlinked-only 设备名称 becomes editable; edit it, then verify it is
      // discarded (reverted) once a repair is bound.
      const nameControl = () => form.locator('.repair-field-control').filter({ hasText: '设备名称' });
      await expectRepairControlCount(form, '设备名称', 1);
      assert.equal(await nameControl().locator('input[data-repair-control]').isEditable(), true, 'unlinked-only field must become editable after clear');
      await nameControl().locator('input[data-repair-control]').fill('临时名称');

      // 5) Bind repair B -> linked view; unlinked-only field hides/reverts as existing.
      await repairField.getByRole('button', { name: '查找', exact: true }).click();
      await repairField.getByText('检修通告B', { exact: true }).waitFor();
      await repairField.locator('label', { hasText: '检修通告B' }).locator('input[type=checkbox]').check();
      assert.equal(await repairField.locator('input[type=checkbox]:checked').count(), 1, 'the new repair must be checked');
      await expectRepairControlCount(form, '设备名称', 0);
      assert.equal(await form.locator('.repair-field-control').filter({ hasText: '设备编号' }).locator('input[data-repair-control]').inputValue(), '手工-1', 'manual number still preserved after binding repair');
      await expectRepairValue(page, form, '故障维修原因', '轴承故障', 'cause retained after binding repair');

      // 6) Clear the event -> previous automatic values are removed but manual input survives.
      await eventField.locator('select').selectOption('__empty__');
      await page.waitForFunction(() => {
        const select = document.querySelector('.plan-field select');
        return select && select.value === '__empty__';
      }, undefined, { timeout: 3000 });
      await expectRepairControlCount(form, '设备名称', 1);
      assert.equal(await nameControl().locator('input[data-repair-control]').inputValue(), '旧设备', 'unlinked-only field must revert to the existing value after clearing');
      await expectRepairValue(page, form, '故障维修原因', '', 'automatic cause must be cleared');
      await expectRepairValue(page, form, '故障发生时间', '', 'automatic date must be cleared');
      assert.equal(await form.locator('.repair-field-control').filter({ hasText: '设备编号' }).locator('input[data-repair-control]').inputValue(), '手工-1', 'manual device number must survive event clearing');
      assert.equal(await specialtySelect().inputValue(), '', 'automatic 所属专业 must be cleared (it was source-forced)');
      await repairField.waitFor();

      // 7) Re-select event B + repair B (repeated switches) preserves manual edits and never
      //    writes native business during the interaction phase.
      await eventField.locator('select').selectOption('rec-event-2');
      await repairField.getByRole('button', { name: '查找', exact: true }).click();
      await repairField.getByText('检修通告B', { exact: true }).waitFor();
      await repairField.locator('label', { hasText: '检修通告B' }).locator('input[type=checkbox]').check();
      await expectRepairControlCount(form, '设备名称', 0);
      assert.equal(await form.locator('.repair-field-control').filter({ hasText: '设备编号' }).locator('input[data-repair-control]').inputValue(), '手工-1', 'manual number preserved across repeated switches');
      await expectRepairValue(page, form, '故障维修原因', '轴承故障', 'cause restored after re-binding');
      await expectRepairValue(page, form, '故障发生时间', NATIVE_DATE_TEXT, 'date restored after re-binding');
      assert.equal(nativeWrites, 0, 'no native repair business write may happen before confirming');
      assert.ok(prefillCalls >= 6, `expected several prefill calls, got ${prefillCalls}`);

      await noRawIds(page);

      // 8) Submit -> new event + new repair with manual + native values preserved.
      await form.getByRole('button', { name: '补充并继续', exact: true }).click();
      await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
      assert.equal(saved.length, 1, 'project submit must happen once');
      const submittedFields = saved[0].values['step0.fields'];
      assert.equal(submittedFields['设备名称'], '旧设备', 'hidden unlinked edit must revert to the existing value on submit');
      assert.equal(submittedFields['设备编号'], '手工-1', 'manual device number must be submitted');
      assert.equal(submittedFields['故障维修原因'], '轴承故障', 'native cause must be submitted');
      assert.equal(submittedFields['故障发生时间'], NATIVE_DATE_TEXT, 'native date (Asia/Shanghai) must be submitted');
      assert.equal(submittedFields['所属专业'], '电气', 'source-forced 所属专业 must be submitted');
      assert.equal(nativeWrites, 0, 'no native write before confirm');

      // 9) Return-edit retains the new event + new repair + manual values.
      await page.getByRole('button', { name: '返回修改', exact: true }).click();
      form = page.locator('.plan-form');
      await form.waitFor();
      assert.equal(await planField(form, '关联事件').locator('select').inputValue(), 'rec-event-2', 'new event retained after return-edit');
      const repairField2 = planField(form, '关联检修通告');
      assert.equal(await repairField2.locator('input[type=checkbox]:checked').count(), 1, 'new repair retained after return-edit');
      assert.equal(await form.locator('.repair-field-control').filter({ hasText: '设备编号' }).locator('input[data-repair-control]').inputValue(), '手工-1', 'manual number retained after return-edit');
      await expectRepairValue(page, form, '故障维修原因', '轴承故障', 'cause retained after return-edit');

      // 10) Reload keeps edit-mode with the changed event and preserved values.
      await page.reload();
      form = page.locator('.plan-form');
      await form.waitFor();
      assert.equal(await planField(form, '关联事件').locator('select').inputValue(), 'rec-event-2', 'new event retained after reload');
      assert.equal(await planField(form, '关联检修通告').locator('input[type=checkbox]:checked').count(), 1, 'new repair retained after reload');
      assert.equal(await form.locator('.repair-field-control').filter({ hasText: '设备编号' }).locator('input[data-repair-control]').inputValue(), '手工-1', 'manual number retained after reload');
      await expectRepairValue(page, form, '故障维修原因', '轴承故障', 'cause retained after reload');
      assert.equal(nativeWrites, 0, 'no native write after reload');

      assert.deepEqual(errors, []);
      await expectNoHorizontalOverflow(page);
      await page.screenshot({ path: path.join(output, `repair-relations-project-${width}.png`), fullPage: true });
      await context.close();
      console.log(`RepairRelationsProject ${width}px: existing-selected/manual-edit-before-switch/first-prefill-fail-preserves-draft/retry-fills-cause+date/source_field_names-override/loading+disabled/repeated-switches/clear-removes-automatic/bind-hides-unlinked/reopen+reload/no-write/no-raw-id OK`);
    }

    // =========================================================== FOLLOWUP
    {
      const context = await browser.newContext({ viewport: { width, height: 1100 }, timezoneId: 'Asia/Shanghai' });
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
        data: { conversation_id: 'repair-relations-followup', configured: true, enabled: true, busy: false,
          turns: [{ operation_id: 'repair-relations-followup-op', question: '更新跟进记录', answer: '请确认跟进记录。', status: 'completed', plan: display }] },
      } }));

      await page.route('**/api/repair-management/cmdb-candidates?*', route => {
        const url = new URL(route.request().url());
        assert.equal(url.searchParams.get('scope'), 'A');
        const q = url.searchParams.get('q') || '';
        const rows = q ? DEVICES.filter(d => d.name.includes(q) || d.device_name.includes(q)) : DEVICES;
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
          if (deviceSearchCount === 1) {
            assert.ok(selected.includes('rec-device-old'), 'first cmdb search must retain the currently selected device');
          }
          editable = withFieldOptions(editable, 'cmdb_record_ids', DEVICES.map(d => ({ value: d.record_id, label: d.name, device_name: d.device_name })));
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
            body: { scope: 'A', summary_record_id: 'rec-parent', cmdb_record_ids: values['step0.cmdb_record_ids'], fields: values['step0.fields'] },
            selected_labels: { 'CMDB设备': '柴油发电机', '楼栋': 'A' },
          }],
        };
        return route.fulfill({ json: { ok: true, data: display } });
      });

      await page.goto(base);
      await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
      let form = page.locator('.plan-form');
      await form.waitFor();

      const cmdbField = planField(form, 'CMDB设备');
      // 11) Original CMDB checked; name/number reflect the original device.
      assert.equal(await cmdbField.locator('input[type=checkbox]').count(), 1, 'one original CMDB option expected');
      assert.equal(await cmdbField.locator('input[type=checkbox]').first().isChecked(), true, 'original CMDB device must be checked');
      const nameField = () => form.locator('.repair-field-control').filter({ hasText: '设备名称' }).locator('input[data-repair-control]');
      const numField = () => form.locator('.repair-field-control').filter({ hasText: '设备编号' }).locator('input[data-repair-control]');
      assert.equal(await nameField().inputValue(), '旧设备');
      assert.equal(await numField().inputValue(), 'OLD-1');

      // 12) Search retains the current selection (asserted in the options handler).
      await cmdbField.getByRole('button', { name: '查找', exact: true }).click();
      await cmdbField.getByText('柴油发电机', { exact: true }).waitFor();

      // 13) Select the new device (multiselect keeps the original): the name/number
      //     fields autofill the joined device names.
      const deviceCheck = (label) => cmdbField.locator('label', { hasText: label }).locator('input[type=checkbox]');
      await deviceCheck('柴油发电机').check();
      assert.equal(await deviceCheck('旧设备').isChecked(), true, 'old device stays checked in multiselect');
      assert.equal(await deviceCheck('柴油发电机').isChecked(), true, 'new device becomes checked');
      assert.equal(await deviceCheck('UPS主机').isChecked(), false, 'unrelated device stays unchecked');
      assert.equal(await nameField().inputValue(), '旧设备、柴油发电机', '设备名称 autofilled with joined device names');
      assert.equal(await numField().inputValue(), '旧设备、柴油发电机', '设备编号 autofilled in sync with device_name');

      // 14) Clear -> selection AND names clear (the option list itself is retained).
      await cmdbField.getByRole('button', { name: '清空CMDB设备', exact: true }).click();
      await deviceCheck('柴油发电机').first().waitFor();
      assert.equal(await deviceCheck('旧设备').isChecked(), false, 'old device unchecked after 清空');
      assert.equal(await deviceCheck('柴油发电机').isChecked(), false, 'new device unchecked after 清空');
      assert.equal(await deviceCheck('UPS主机').isChecked(), false, 'unrelated device unchecked after 清空');
      assert.equal(await nameField().inputValue(), '', '设备名称 must clear after 清空');
      assert.equal(await numField().inputValue(), '', '设备编号 must clear after 清空');

      // 15) Re-select only the new device (selection was cleared), then manual
      //     override the name.
      await deviceCheck('柴油发电机').check();
      assert.equal(await deviceCheck('旧设备').isChecked(), false, 'old device stays cleared');
      assert.equal(await deviceCheck('柴油发电机').isChecked(), true, 'only the new device is selected after clear + reselect');
      assert.equal(await deviceCheck('UPS主机').isChecked(), false, 'unrelated device stays unchecked');
      assert.equal(await nameField().inputValue(), '柴油发电机');
      await nameField().fill('手动设备名');

      await noRawIds(page);

      // 16) Submit -> cmdb id + manual override.
      await form.getByRole('button', { name: '补充并继续', exact: true }).click();
      await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
      assert.equal(saved.length, 1, 'followup submit must happen once');
      assert.deepEqual(saved[0].values['step0.cmdb_record_ids'], ['rec-device-0'], 'submitted cmdb id must be the new device');
      assert.equal(saved[0].values['step0.fields']['设备名称'], '手动设备名', 'manual override must be retained on submit');

      // 17) Return-edit retains autofill + manual override + new device selection.
      await page.getByRole('button', { name: '返回修改', exact: true }).click();
      form = page.locator('.plan-form');
      await form.waitFor();
      assert.equal(await form.locator('.repair-field-control').filter({ hasText: '设备名称' }).locator('input[data-repair-control]').inputValue(), '手动设备名', 'override retained after return-edit');
      assert.equal(await planField(form, 'CMDB设备').locator('label', { hasText: '柴油发电机' }).locator('input[type=checkbox]').isChecked(), true, 'new device still checked after return-edit');

      // 18) Reload keeps edit-mode and the manual override.
      await page.reload();
      form = page.locator('.plan-form');
      await form.waitFor();
      assert.equal(await form.locator('.repair-field-control').filter({ hasText: '设备名称' }).locator('input[data-repair-control]').inputValue(), '手动设备名', 'override retained after reload');

      assert.deepEqual(errors, []);
      await expectNoHorizontalOverflow(page);
      await page.screenshot({ path: path.join(output, `repair-relations-followup-${width}.png`), fullPage: true });
      await context.close();
      console.log(`RepairRelationsFollowup ${width}px: original-checked/search-retains-selection/new-device-autofill/clear-clears/manual-override/submit/return-edit/reload/no-raw-id OK`);
    }
  }
} finally {
  await browser.close();
}