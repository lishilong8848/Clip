import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const python = path.join(root, 'bin/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
const fixtures = JSON.parse(execFileSync(python, ['-c', `import asyncio,json
from bin.test_lighthouse_cabinet_text_workflows import CabinetTextWorkflowTests, CabinetMissingBatchPickerTests
from bin.lan_bitable_template_portal.cabinet_power_excel import system_name
t=CabinetTextWorkflowTests();t.setUpClass()
def line(rack,action='上正式电',day='03',room='202'):
    return f'{system_name("E",room)} {rack} {action} 2026-10-{day} 10:00:00 2026-10-02 09:05:00'
async def derive():
    await t.asyncSetUp()
    base='\\n'.join((line('A11'),line('A12'),line('B17','下测试电'),line('Z99','上测试电')))
    conflict=line('A11',day='04')
    batch=t._create_batch([{'scope':'E','room':'202','rack':rack,'action':'上测试电','result':'成功'} for rack in ('A11','A12','B17','B17')])
    plan,_=t._prepare(batch['batch_id'],[])
    sources=[{'id':'browser_source_a','text':base},{'id':'browser_source_b','text':conflict}]
    previews=[t.service.batches.preview_text_fill(batch['batch_id'],{'sources':sources[:count]},t.actor['id'],['E']) for count in (1,2)]
    inventory=t.service._snapshot('E')['config']['inventory'][:500]
    large=t._create_batch([{'scope':'E','room':rack['room'],'rack':rack['rack'],'action':'上测试电','result':'成功'} for rack in inventory])
    large_sources=[{'id':'browser_large_source','text':'\\n'.join(line(rack['rack'],room=rack['room']) for rack in inventory)}]
    large_plan,_=t._prepare(large['batch_id'],large_sources)
    large_preview=await t._preview(large_plan)
    create_plan=t.agent.prepare(t.actor,{'operations':[{'api_id':'POST /api/cabinet-power/batches','body':{'source':'text'}}]},'widget-text-create',[])
    create_text='\\n'.join((line('A11')+' A11 Success',line('A12')+' A12 Success'))
    create_preview=t.service.batches.text_preview([{'id':'widget_create_source','text':create_text}],['E'])
    p=CabinetMissingBatchPickerTests()
    await p.asyncSetUp()
    picker_plan=p._prepare_picker()
    p.batch_pages[('A',1)]=[{'batch_id':'A-1001','title':'A楼晨会批次','rooms':['202'],'pending_rows':3,'created_at':'2026-10-01 08:00:00'}]
    p.batch_pages[('E',1)]=[{'batch_id':'E-2001','title':'E楼巡检批次','rooms':['501'],'pending_rows':7,'created_at':'2026-10-01 09:00:00'}]
    fname=(p._picker_field(picker_plan))['name']
    options_plan=await p.agent.field_options(p.actor,picker_plan['id'],fname,p._req())
    options_search_plan=await p.agent.field_options(p.actor,picker_plan['id'],fname,p._req(b'q=E-2001'))
    amended=p.agent.amend(p.actor,picker_plan['id'],{'version':options_search_plan['version'],'values':{fname:'E-2001'}})
    # Coherent one-line preview for the retained picker source (single LARGE_LINE),
    # so the embedded form rendered after the picker PATCH matches Stage-A reality.
    picker_preview=t.service.batches.preview_text_fill(batch['batch_id'],{'sources':p.sources},t.actor['id'],['E'])
    for agent in (p.agent,):
        if agent.tasks:await asyncio.gather(*tuple(agent.tasks))
    for callback,args,kwargs in reversed(p._cleanups):callback(*args,**kwargs)
    p._cleanups.clear()
    print(json.dumps({'plan':t.agent.public_plan(plan),'base':base,'conflict':conflict,'previews':previews,
        'large_plan':t.agent.public_plan(large_plan),'large_preview':large_preview,
        'picker_plan':picker_plan,'options_plan':options_plan,'options_search_plan':options_search_plan,'amended':amended,
        'picker_preview':picker_preview,'create_plan':t.agent.public_plan(create_plan),'create_text':create_text,'create_preview':create_preview},ensure_ascii=False))
try:asyncio.run(derive())
finally:
    for callback,args,kwargs in reversed(t._cleanups):callback(*args,**kwargs)
    t._cleanups.clear()`], { cwd: root, env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' }, encoding: 'utf8', maxBuffer: 8 * 1024 * 1024 }));
const output = path.join(root, 'output/playwright/assistant-cabinet-text');
await mkdir(output, { recursive: true });
const base = 'http://127.0.0.1:19003';
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage(), errors = [], saves = [], previews = [], nativeCalls = [], pickerSaves = [], optionQueries = [];
    page.on('pageerror', error => errors.push(error.message));
    let large = false, creating = false, pickerStage = true, plan = structuredClone(fixtures.picker_plan);
    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: { ok: true, data: {
      conversation_id: 'cabinet-text-fixture', configured: true, enabled: true, busy: false,
      turns: [{ operation_id: 'cabinet-text', question: '给这个批次补填文本', answer: '请粘贴并核对记录。', status: 'completed', plan }],
    } } }));
    await page.route('**/api/cabinet-power/**', route => { nativeCalls.push(route.request().method()); return route.abort(); });
    // options requests carry ?field=...&q=... which a trailing-glob (`plans/*/options`)
    // cannot match against the full URL, so match on pathname and read q from the query string.
    await page.route(url => {
      const path = new URL(url).pathname;
      return path.startsWith('/api/assistant/plans/') && path.endsWith('/options');
    }, async route => {
      const q = new URL(route.request().url()).searchParams.get('q') || '';
      optionQueries.push(q);
      plan = structuredClone(q ? fixtures.options_search_plan : fixtures.options_plan);
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.route(url => new URL(url).pathname.endsWith('/cabinet-text-preview'), async route => {
      const body = route.request().postDataJSON();
      assert.equal(body.field, plan.fields[0].name);
      assert.equal(body.version, plan.version);
      previews.push(body);
      // picker stage embeds the single one-line LARGE_LINE source retained from
      // CabinetMissingBatchPickerTests; return its coherent 1-row preview rather
      // than the unrelated 4-line Stage-B fixture.
      const pickerSource = body.sources.length === 1 && body.sources[0].id === 'picker-source';
      const data = structuredClone(creating ? fixtures.create_preview : pickerSource ? fixtures.picker_preview : large ? fixtures.large_preview : fixtures.previews[body.sources.length - 1]);
      if (creating || pickerSource) for (const row of data.rows) row.text_id = body.sources[0].id;
      else if (!large) for (const row of data.rows) row.text_id = body.sources[row.text_id === 'browser_source_a' ? 0 : 1].id;
      await new Promise(resolve => setTimeout(resolve, 200));
      return route.fulfill({ json: { ok: true, data } });
    });
    await page.route('**/api/assistant/plans/*', route => {
      assert.equal(route.request().method(), 'PATCH');
      const body = route.request().postDataJSON();
      if (pickerStage) {
        pickerSaves.push(body);
        assert.deepEqual(body.values, { 'step0.batch_id': 'E-2001' });
        plan = structuredClone(fixtures.amended);
        pickerStage = false;
        return route.fulfill({ json: { ok: true, data: fixtures.amended } });
      }
      saves.push(body);
      plan = { ...plan, status: 'awaiting_confirmation', fields: [], version: plan.version + 1,
        ...(creating ? { operations: [{ api_id: 'POST /api/cabinet-power/batches', name: '创建机柜待办',
          body: { source: 'text', ...body.values['step0.text_create'] }, selected_labels: { rows: '1 条机柜记录', sources: '1 次粘贴' } }] } : {}) };
      return route.fulfill({ json: { ok: true, data: plan } });
    });
    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    const form = page.locator('.plan-form'), submit = form.getByRole('button', { name: '补充并继续', exact: true });
    const editor = form.locator('.text-fill');

    // ---- Stage A: missing batch_id -> CabinetMissingBatchPickerTests picker ----
    const picker = form.getByLabel('选择现有机柜待办批次', { exact: true });
    await picker.waitFor();
    assert.equal(await picker.locator('option').count(), 1, '待选批次尚未读取，仅供“请选择”');
    assert.equal(pickerSaves.length, 0);
    // load options (prepare -> field_options with actor A-E scopes)
    await form.getByRole('button', { name: '查找', exact: true }).click();
    // Native <select> options are not "visible" until the dropdown opens; wait on
    // attached/count instead of visible so the fixture options are matched reliably.
    await picker.locator('option[value="A-1001"]').waitFor({ state: 'attached' });
    await picker.locator('option[value="E-2001"]').waitFor({ state: 'attached' });
    const optionsText = await picker.locator('option').allTextContents();
    assert.ok(optionsText.some(t => t.includes('A楼晨会批次') && t.includes('2026-10-01 08:00:00') && t.includes('3条待办') && t.includes('A-1001'))); // A scope, date, pending
    assert.ok(optionsText.some(t => t.includes('E楼巡检批次') && t.includes('2026-10-01 09:00:00') && t.includes('7条待办') && t.includes('E-2001'))); // E scope, date, pending
    // search narrows across pages to the matching batch
    await form.getByLabel('查找可选记录', { exact: true }).fill('E-2001');
    await form.getByRole('button', { name: '查找', exact: true }).click();
    await picker.locator('option[value="A-1001"]').waitFor({ state: 'detached' });
    assert.equal(await picker.locator('option[value="E-2001"]').count(), 1);
    assert.deepEqual(optionQueries, ['', 'E-2001']);
    // choose via native select control, then a single PATCH returns needs_input native_cabinet_text_fill
    await picker.selectOption('E-2001');
    await page.screenshot({ path: path.join(output, `picker-${width}.png`), fullPage: true });
    await submit.click();
    // The single PATCH must transition to the native needs_input text form. Assert the
    // rendered DOM (not the fixture object) before reloading: the embedded .text-fill is
    // present and its coherent one-line preview proves the retained picker source renders.
    assert.equal(pickerSaves.length, 1);
    assert.equal(saves.length, 0, '批次选择不得触发文本提交');
    assert.equal(fixtures.amended.status, 'needs_input');
    assert.equal(fixtures.amended.fields[0].name, 'step0.text_fill');
    assert.equal(fixtures.amended.fields[0].native_cabinet_text_fill, true);
    assert.equal(fixtures.amended.fields[0].batch_id, 'E-2001');
    await editor.waitFor();
    await editor.getByText('识别 1 条 · 可填入 1 条', { exact: true }).waitFor();
    assert.equal(await editor.getByLabel('机柜上下电记录', { exact: true }).inputValue(), '', '粘贴框保持为空，源经预览汇入表单');
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), true);

    // ---- Stage B: existing native text paste/preview + 500-row (preserved) ----
    plan = structuredClone(fixtures.plan);
    await page.reload();
    await editor.waitFor();
    assert.equal(await submit.isDisabled(), true);
    assert.equal(await editor.getByRole('button', { name: /^填入本批次/ }).count(), 0, '嵌入模式不得直接写入');
    const paste = value => editor.getByLabel('机柜上下电记录', { exact: true }).evaluate((el, text) => {
      const clipboardData = new DataTransfer(); clipboardData.setData('text/plain', text);
      el.dispatchEvent(new ClipboardEvent('paste', { clipboardData, bubbles: true, cancelable: true }));
    }, value);
    await paste(fixtures.base);
    await editor.getByText('正在匹配本批次机柜…', { exact: true }).waitFor();
    assert.equal(await submit.isDisabled(), true);
    await editor.getByText('识别 4 条 · 可填入 2 条', { exact: true }).waitFor();
    assert.equal(await editor.getByLabel('机柜上下电记录', { exact: true }).inputValue(), '');
    await editor.getByText('同柜多条，请在待办中核对', { exact: true }).waitFor();
    await editor.getByText('本批次无此机柜', { exact: true }).waitFor();
    await paste(fixtures.conflict);
    await editor.getByText('识别 5 条 · 可填入 1 条', { exact: true }).waitFor();
    assert.equal(await editor.getByText('内容冲突，请移除错误记录', { exact: true }).count(), 2);
    await editor.locator('tbody tr').filter({ hasText: '2026-10-04' }).getByRole('button', { name: '移除识别记录 A11' }).click();
    await editor.getByText('识别 4 条 · 可填入 2 条', { exact: true }).waitFor();
    await editor.getByRole('button', { name: '移除识别记录 A12' }).click();
    await editor.getByText('识别 3 条 · 可填入 1 条', { exact: true }).waitFor();
    assert.equal(saves.length, 0, '识别/移除不提交助手表单');
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), true);
    await page.screenshot({ path: path.join(output, `preview-${width}.png`), fullPage: true });
    await submit.click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    const value = saves.at(-1).values[fixtures.plan.fields[0].name];
    assert.equal(value.rows.length, 1);
    assert.equal(value.rows[0].row_id, fixtures.previews[0].rows[0].targets[0].row_id);
    assert.equal(value.sources.length, 2);
    assert.equal(value.omitted.length, 2);
    large = true; plan = structuredClone(fixtures.large_plan);
    await page.reload();
    await editor.getByText('识别 500 条 · 可填入 500 条', { exact: true }).waitFor();
    assert.equal(await editor.locator('tbody tr').count(), 25);
    await editor.getByRole('button', { name: '下一页', exact: true }).click();
    assert.equal(await editor.locator('tbody tr').count(), 25);
    assert.equal(saves.length, 1);
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), true);
    await page.screenshot({ path: path.join(output, `500-rows-${width}.png`), fullPage: true });
    await submit.click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.equal(saves.at(-1).values[fixtures.large_plan.fields[0].name].rows.length, 500, '提交所有选中页，而不是只提交当前页');

    // ---- Stage C: a new text batch uses the same native parser, not raw JSON. ----
    creating = true; plan = structuredClone(fixtures.create_plan);
    await page.reload();
    const creator = form.locator('.lh-text-create');
    await creator.waitFor();
    assert.equal(await submit.isDisabled(), true);
    await creator.getByLabel('粘贴机柜确认文本', { exact: true }).evaluate((el, text) => {
      const clipboardData = new DataTransfer(); clipboardData.setData('text/plain', text);
      el.dispatchEvent(new ClipboardEvent('paste', { clipboardData, bubbles: true, cancelable: true }));
    }, fixtures.create_text);
    await page.waitForFunction(() => document.querySelectorAll('.lh-text-create tbody tr').length === 2);
    await creator.getByRole('button', { name: '移除记录 A12', exact: true }).click();
    await creator.getByLabel('实际完成时间 A11', { exact: true }).fill('2026-10-02T09:10:23');
    await creator.getByLabel('结果 A11', { exact: true }).selectOption('失败');
    const count = saves.length;
    await submit.click();
    assert.equal(saves.length, count, '失败原因未填写不得进入提交确认');
    await creator.getByLabel('失败原因 A11', { exact: true }).fill('测试确认原因');
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), true);
    await page.screenshot({ path: path.join(output, `create-${width}.png`), fullPage: true });
    await submit.click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    const created = saves.at(-1).values['step0.text_create'];
    assert.equal(created.rows.length, 1);
    assert.equal(created.rows[0].rack, 'A11');
    assert.equal(created.rows[0].actual, '2026-10-02 09:10:23');
    assert.equal(created.rows[0].failure_reason, '测试确认原因');
    assert.equal(created.rows[0].text_id, created.sources[0].id);
    assert((await page.locator('.plan-steps').innerText()).includes('尚不写入正式台账'));
    assert.deepEqual(nativeCalls, []);
    assert.deepEqual(errors, []);
    await context.close();
    console.log(`Assistant cabinet text ${width}px: batch picker (scope/date/pending + search + select PATCH) then native preview, conflict/ambiguity, selected rows, 500-row paging and no direct write OK`);
  }
} finally { await browser.close(); }
