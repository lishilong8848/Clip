import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../..', 'output/playwright/notice-panel');
await mkdir(output, { recursive: true });
const server = await preview({ root, logLevel: 'error', preview: { host: '127.0.0.1', port: 0, strictPort: false } });
const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1366, height: 900 }, timezoneId: 'UTC' });
page.setDefaultTimeout(12000);
const errors = [], requests = [], docs = new Map();
let edition = 'morning', day = '2026-10-09', confirmed = 0, conflict = false, polls = 0, sopReads = 0, sopVersion = 3, sopFail = false;
const base = '/api/assistant/notice-panel';
const field = (key, label, extra = {}) => ({ key, label, kind: 'text', options: [], required: true, readonly: false, ...extra });
function rows() {
  const rows = Array.from({ length: 12 }, (_, i) => ({ key: `n${i}`, title: i === 1 ? 'E楼空调运行模式调整' : `E楼${i + 1}号冷水机组月度维保`,
    work_type: i === 1 ? 'adjust' : 'maintenance', action: i === 1 ? 'update' : 'start', status: i === 1 ? '进行中' : '未开始',
    window: '2026-10-09 至 2026-10-20', selected: false, edit_all: false, brief_fields: [], blocked: '',
    fields: [field('title', '通告名称', { readonly: true }), ...(i === 1 ? [field('notice_action', '本次操作', { kind: 'select', options: ['更新', '结束'] })] : [field('progress', '本次进度', { kind: 'multiline' })]), field('content', '内容', { kind: 'multiline' }), ...(i === 0 ? [field('start_time', '开始时间（YYYY-MM-DD HH:mm）'), field('end_time', '结束时间（YYYY-MM-DD HH:mm）'), field('notice_sop', '本次工单', { kind: 'notice_sop' })] : [])],
    draft: { title: '机组维保', progress: '', content: '按周期维护设备', notice_action: '更新', ...(i === 0 ? { start_time: '2026-10-09 09:30', end_time: '2026-10-09 18:30', notice_sop: { exempt: false, scope: '', sop_id: '', sop_version: 0, operator_record_id: '', reviewer_record_id: '', runs: [] } } : {}) }, phase: '', result: '', error: '', preview: '' }));
  const END_DEFAULT = '工作已完成，设备运行正常，请知晓！';
  rows.push({ key: 'n12', title: 'E楼回风机组月度维保', work_type: 'maintenance', action: 'update', status: '进行中',
    window: '2026-10-09 至 2026-10-20', selected: false, edit_all: false, brief_fields: [], blocked: '',
    fields: [field('title', '通告名称', { readonly: true }), field('notice_action', '本次操作', { kind: 'select', options: ['更新', '结束'] }), field('progress', '本次进度', { kind: 'multiline', default_on_end: END_DEFAULT }), field('content', '内容', { kind: 'multiline' })],
    draft: { title: '回风机组维保', progress: '', content: '按周期维护回风机组', notice_action: '更新' }, phase: '', result: '', error: '', preview: '' });
  return rows;
}
page.on('pageerror', e => errors.push(e.message));
page.on('dialog', dialog => dialog.accept());
await page.route('**/assistant.html*', async route => {
  const response = await route.fetch();
  await route.fulfill({ response, body: (await response.text()).replace('id="clipflow-lighthouse-widget"', 'id="clipflow-lighthouse-widget" data-user-id="notice-fixture" data-user-name="隔离测试"') });
});
await page.route('**/api/**', async route => {
  const url = new URL(route.request().url()), method = route.request().method();
  requests.push([method, url.pathname]);
  const reply = (data, status = 200) => route.fulfill({ status, json: { ok: true, data } });
  if (url.pathname === '/api/assistant/conversation') return reply({ conversation_id: 'fixture', enabled: true, configured: false, busy: false,
    turns: [{ operation_id: 'fixture-answer', question: '今天有哪些通告需要处理？', answer: '### 今日通告\n\n当前有 **2 条待开始**、**1 条进行中**。\n\n| 通告 | 状态 |\n|---|---|\n| 冷水机组月度维保 | 待开始 |\n| 空调运行模式调整 | 进行中 |\n\n以上为隔离测试数据。', status: 'completed', model_name: '测试模型', sources: [] }], model_name: 'WanWu/Deepseek-Auto', model_options: [{ name: 'WanWu/Deepseek-Auto' }], can_manage_settings: true });
  if (url.pathname === base) return reply({ edition: { id: day + '-' + edition, date: day, slot: edition, label: edition === 'morning' ? '08:00通告待办' : '17:00通告待办' }, scopes: [{ value: 'E', label: 'E楼' }, { value: 'A', label: 'A楼' }] });
  if (url.pathname === base + '/open') {
    const body = route.request().postDataJSON(), id = `${day}-${body.scope}-${body.slot || edition}`;
    if (!docs.has(id)) docs.set(id, { id, revision: 1, scope: body.scope, date: day, slot: body.slot || edition, state: 'preparing', items: [], error: '' });
    return reply({ run: structuredClone(docs.get(id)) }, 202);
  }
  if (url.pathname.startsWith(base + '/')) {
    const id = url.pathname.slice(base.length + 1).split('/')[0], run = docs.get(id);
    assert.ok(run, 'read only a created run');
    if (url.pathname.endsWith('/sop-options')) {
      sopReads++;
      await new Promise(resolve => setTimeout(resolve, 250));
      if (sopFail) { sopFail = false; return route.fulfill({ status: 503, json: { ok: false, error: '隔离测试：工单目录暂不可用' } }); }
      return reply({ field: { notice_scope: run.scope, directory_scope: run.scope, work_type: 'maintenance',
        sops: [{ sop_id: 'sop-one', name: '冷水机组维保 SOP', version: sopVersion, steps: [{ content: '现场检查', order: 1 }], documents: [{ name: '维保操作.pdf' }] }],
        people: [{ record_id: 'operator', name: '测试操作人', label: '测试操作人' }, { record_id: 'reviewer', name: '测试审核人', label: '测试审核人' }] } });
    }
    if (method === 'GET') {
      polls++;
      if (run.state === 'preparing') { run.state = 'edit'; run.items = rows(); run.revision++; }
      else if (run.state === 'running') { run.state = 'done'; for (const row of run.items.filter(row => row.selected)) { row.phase = 'success'; row.result = '发送成功'; } run.revision++; }
      return reply({ run: structuredClone(run) });
    }
    const body = route.request().postDataJSON();
    if (conflict) { conflict = false; run.revision++; return route.fulfill({ status: 409, json: { ok: false, error: '其他窗口已更新草稿，请读取最新。', data: { run } } }); }
    assert.equal(body.revision, run.revision);
    for (const change of body.changes || []) { const row = run.items.find(row => row.key === change.key); if (!row.brief_fields.length) row.brief_fields = row.fields.filter(f => f.key === 'notice_action' || !row.draft[f.key]).map(f => f.key); Object.assign(row.draft, change.draft); row.selected = change.selected; row.edit_all = change.edit_all; }
    if (body.action === 'preview') { run.state = 'confirm'; for (const row of run.items.filter(row => row.selected)) row.preview = `【${row.work_type === 'adjust' ? '设备调整' : '维保通告'}】${row.draft.notice_action}\n${row.title}\n${row.draft.progress}\n${row.draft.content}`; }
    else if (body.action === 'confirm') { assert.equal(run.state, 'confirm'); confirmed++; run.state = 'running'; }
    else if (body.action === 'edit') run.state = 'edit';
    else if (body.action === 'refresh') { run.items = rows(); run.state = 'edit'; }
    run.revision++;
    return reply({ run: structuredClone(run) });
  }
  assert.ok(!url.pathname.includes('notice-cards') && url.pathname !== '/api/workbench-actions', 'no retired transport or direct browser business execution');
  return reply({});
});
async function visible(selector) { await page.locator(selector).first().waitFor({ state: 'visible' }); }
try {
  await page.goto(`http://127.0.0.1:${server.httpServer.address().port}/assistant.html`);
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  await visible('.notice-panel');
  await page.getByLabel('楼栋', { exact: true }).selectOption('E');
  await visible('.notice-row');
  assert.equal(await page.locator('.notice-row').count(), 10);
  assert.equal(await page.getByRole('tab', { name: '未开始计划', exact: true }).getAttribute('aria-selected'), 'true');
  assert.equal(await page.getByText('E楼空调运行模式调整', { exact: true }).count(), 0, 'ongoing notices are not mixed into plans');
  assert.equal(await page.locator('.notice-fields').count(), 0);
  await page.locator('.row-heading input[type=checkbox]').first().check();
  await page.getByLabel('本次进度', { exact: false }).fill('工程师已到场，准备维保');
  const selectedStyle = await page.locator('.notice-row').first().evaluate(el => ({ selected: el.classList.contains('selected'), shadow: getComputedStyle(el).boxShadow }));
  assert.equal(selectedStyle.selected, true);
  assert.notEqual(selectedStyle.shadow, 'none', 'selected notice needs an explicit visual indicator');
  assert.equal(await page.getByLabel('本次进度', { exact: false }).count(), 1, 'typing cannot hide the field');
  assert.equal(await page.getByLabel(/^内容/).count(), 0);
  await page.getByRole('button', { name: '编辑全部字段', exact: true }).click();
  await page.getByLabel(/^内容/).fill('检查冷水机组电气及机械部件');
  const start = page.locator('input[type="datetime-local"]').first(), end = page.locator('input[type="datetime-local"]').nth(1);
  assert.equal(await start.getAttribute('type'), 'datetime-local');
  assert.equal(await end.getAttribute('type'), 'datetime-local');
  assert.equal(await start.inputValue(), '2026-10-09T09:30', 'no timezone conversion even in UTC browser');
  await page.getByRole('button', { name: '选择开始时间', exact: true }).click();
  await page.keyboard.press('Escape');
  await start.fill('2026-10-09T11:00');
  await end.fill('2026-10-09T19:00');
  await start.scrollIntoViewIfNeeded();
  await page.screenshot({ path: path.join(output, 'datetime-pickers.png') });
  const sop = page.getByRole('combobox', { name: '工单SOP', exact: true });
  await sop.selectOption('sop-one');
  await page.getByRole('combobox', { name: '操作人', exact: true }).click();
  await page.getByRole('option', { name: '测试操作人', exact: true }).click();
  await page.getByRole('combobox', { name: '现场审核人', exact: true }).click();
  await page.getByRole('option', { name: '测试审核人', exact: true }).click();
  assert.equal(await page.locator('.lhs-steps').count(), 0, 'steps collapsed by default');
  await page.getByRole('button', { name: '1 个步骤 · 1 份附件', exact: true }).click();
  assert.equal(await page.locator('.lhs-attach-list').innerText(), '维保操作.pdf');
  await page.getByRole('button', { name: '1 个步骤 · 1 份附件', exact: true }).click();
  await page.screenshot({ path: path.join(output, 'date-and-sop.png') });
  const openRequests = requests.filter(([method, url]) => method === 'POST' && url === base + '/open').length;
  const combinedWidth = (await page.locator('.assistant-shell').boundingBox()).width;
  const chatBeforeCollapse = await page.locator('.assistant-panel').boundingBox();
  await page.getByRole('button', { name: '侧边栏（通告待办 / 知识库）', exact: true }).click();
  await page.waitForFunction(() => document.querySelector('.assistant-sidebar').getBoundingClientRect().width === 0);
  assert.notEqual(await page.locator('.assistant-sidebar').getAttribute('inert'), null);
  assert.equal(await page.getByRole('button', { name: '侧边栏（通告待办 / 知识库）', exact: true }).getAttribute('aria-expanded'), 'false');
  assert.equal(await page.evaluate(() => document.activeElement.classList.contains('sidebar-toggle')), true);
  assert.ok((await page.locator('.assistant-shell').boundingBox()).width < combinedWidth - 200);
  const chatAfterCollapse = await page.locator('.assistant-panel').boundingBox();
  assert.ok(Math.abs(chatBeforeCollapse.x + chatBeforeCollapse.width - chatAfterCollapse.x - chatAfterCollapse.width) < 2, 'chat edge stays anchored beside launcher');
  assert.equal(await page.locator('.notice-panel').count(), 1, 'collapse keeps the draft component mounted');
  await page.screenshot({ path: path.join(output, 'chat-collapsed.png') });
  await page.getByRole('button', { name: '侧边栏（通告待办 / 知识库）', exact: true }).press('Enter');
  await page.getByLabel('本次进度', { exact: false }).waitFor();
  assert.equal(await page.getByLabel('本次进度', { exact: false }).inputValue(), '工程师已到场，准备维保');
  assert.equal(await page.getByLabel(/^内容/).inputValue(), '检查冷水机组电气及机械部件');
  assert.equal(await start.inputValue(), '2026-10-09T11:00');
  assert.equal(await sop.inputValue(), 'sop-one');
  assert.equal(requests.filter(([method, url]) => method === 'POST' && url === base + '/open').length, openRequests, 'reopening does not recreate the run');
  await page.locator('.row-heading input[type=checkbox]').first().uncheck();
  await page.getByRole('button', { name: '保存草稿', exact: true }).click();
  await page.waitForFunction(() => !document.querySelector('.notice-panel[aria-busy="true"]'));
  const saved = docs.get(`${day}-E-morning`).items[0].draft;
  assert.equal(saved.start_time, '2026-10-09 11:00');
  assert.equal(saved.end_time, '2026-10-09 19:00');
  assert.deepEqual(saved.notice_sop, { exempt: false, scope: 'E', sop_id: 'sop-one', sop_version: 3, operator_record_id: 'operator', reviewer_record_id: 'reviewer', runs: [] });
  await page.locator('.row-heading input[type=checkbox]').first().check();
  assert.equal(await page.getByLabel('本次进度', { exact: false }).inputValue(), '工程师已到场，准备维保');
  assert.equal(await sop.inputValue(), 'sop-one');
  await page.getByRole('button', { name: '保存草稿', exact: true }).click();
  await page.getByLabel('楼栋', { exact: true }).selectOption('A');
  await visible('.notice-row');
  await page.getByLabel('楼栋', { exact: true }).selectOption('E');
  await page.getByLabel('本次进度', { exact: false }).waitFor();
  assert.equal(await page.getByLabel('本次进度', { exact: false }).inputValue(), '工程师已到场，准备维保');
  await page.getByRole('tab', { name: '进行中通告', exact: true }).click();
  await page.getByText('E楼空调运行模式调整', { exact: true }).waitFor();
  assert.equal(await page.locator('.notice-row').count(), 2);
  assert.equal(await page.locator('.row-heading input:checked').count(), 0, 'view switch clears previous selection');
  await page.locator('.row-heading input[type=checkbox]').first().check();
  await page.getByLabel(/^本次操作/).selectOption('结束');
  assert.equal(await page.locator('.notice-row').first().getByText('本次进度').count(), 0, 'adjust notices do not gain progress');
  await page.getByRole('button', { name: '预览所选 1 条', exact: true }).click();
  await visible('.notice-preview');
  assert.equal(await page.locator('.notice-preview').count(), 1, 'hidden plans must not be submitted with ongoing notices');
  assert.equal(await page.getByRole('tab', { name: '未开始计划', exact: true }).isDisabled(), true, 'confirmation stays on its own batch');
  await page.screenshot({ path: path.join(output, 'ongoing-page.png') });
  await page.getByRole('button', { name: '返回修改', exact: true }).click();
  await page.getByRole('tab', { name: '未开始计划', exact: true }).click();
  const typeSwitchReads = requests.filter(([, url]) => url === base + '/open').length;
  await page.getByRole('checkbox', { name: '选择E楼1号冷水机组月度维保', exact: true }).check();
  assert.equal(await page.getByLabel('本次进度', { exact: false }).inputValue(), '工程师已到场，准备维保', 'view switching preserves fields');
  assert.equal(await sop.inputValue(), 'sop-one');
  await page.locator('.row-heading input[type=checkbox]').nth(1).check();
  await page.locator('.notice-row').nth(1).getByLabel('本次进度', { exact: false }).fill('另一条计划已准备');
  await page.getByRole('button', { name: '预览所选 2 条', exact: true }).click();
  await visible('.notice-preview');
  assert.equal(confirmed, 0);
  assert.equal(await page.locator('.notice-preview').count(), 2);
  assert.equal(docs.get(`${day}-E-morning`).items.find(row => row.key === 'n1').selected, false, 'ongoing selection excluded from planned submissions');
  assert.equal(requests.filter(([, url]) => url === base + '/open').length, typeSwitchReads, 'switching category uses local rows, without refetching');
  await page.screenshot({ path: path.join(output, 'preview-1366.png') });
  await page.getByRole('button', { name: '确认发送 2 条', exact: true }).click();
  await page.getByRole('button', { name: '继续办理其他通告', exact: true }).waitFor();
  assert.equal(confirmed, 1);
  await page.getByRole('button', { name: '继续办理其他通告', exact: true }).click();
  await page.locator('.row-heading input[type=checkbox]').first().check();
  await page.getByLabel('本次进度', { exact: false }).fill('尚未提交的上午草稿');
  edition = 'evening';
  await page.evaluate(() => document.dispatchEvent(new Event('visibilitychange')));
  await page.waitForFunction(() => document.querySelector('.notice-select select[aria-label="时段"]')?.value === 'evening');
  await visible('.notice-row');
  assert.equal(await page.getByRole('tab', { name: '进行中通告', exact: true }).getAttribute('aria-selected'), 'true', '17:00 defaults to ongoing page');
  assert.equal(await page.locator('.edition-update').count(), 0, 'scheduled switch needs no manual update button');
  await page.getByLabel('时段', { exact: true }).selectOption('morning');
  await page.getByLabel('本次进度', { exact: false }).waitFor();
  assert.equal(await page.getByLabel('本次进度', { exact: false }).inputValue(), '尚未提交的上午草稿');
  conflict = true;
  await page.getByRole('button', { name: '保存草稿', exact: true }).click();
  await page.getByRole('button', { name: '读取最新草稿', exact: true }).click();
  await page.getByRole('button', { name: '读取最新草稿', exact: true }).waitFor({ state: 'hidden' });
  await page.locator('.row-heading input[type=checkbox]').first().check();
  await page.getByLabel('本次进度', { exact: false }).fill('冲突后重新填写');
  await page.getByRole('button', { name: '保存草稿', exact: true }).click();
  await page.getByRole('button', { name: '收起助手', exact: true }).click();
  await page.locator('.notice-panel').waitFor({ state: 'hidden' });
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  await page.waitForFunction(() => document.querySelector('.notice-select select[aria-label="时段"]')?.value === 'evening');
  await page.getByLabel('时段', { exact: true }).selectOption('morning');
  await page.getByLabel('本次进度', { exact: false }).waitFor();
  assert.equal(await page.getByLabel('本次进度', { exact: false }).inputValue(), '冲突后重新填写');
  for (const width of [1920, 1366, 1024]) {
    await page.setViewportSize({ width, height: 900 });
    await page.waitForTimeout(450);
    const boxes = await page.evaluate(() => ['.assistant-panel', '.notice-panel', '.assistant-launcher'].map(s => { const r = document.querySelector(s).getBoundingClientRect(); return { x: r.x, y: r.y, right: r.right, bottom: r.bottom, width: r.width }; }));
    for (const box of boxes) assert.ok(box.x >= -1 && box.right <= width + 1 && box.bottom <= 901, `within ${width}: ${JSON.stringify(boxes)}`);
    assert.ok(Math.abs(boxes[1].right - boxes[0].x) <= 1, 'left sidebar must join the chat without a gap');
    assert.ok(boxes.slice(0, 2).every(b => b.right <= boxes[2].x || b.x >= boxes[2].right), 'launcher remains unobscured');
    const metrics = await page.evaluate(() => {
      const shell = document.querySelector('.assistant-shell'), chat = document.querySelector('.assistant-panel'), panel = document.querySelector('.notice-panel');
      const header = chat.querySelector('.assistant-header'), noticeHeader = panel.querySelector('.notice-head');
      return { radii: [shell, chat, panel].map(e => getComputedStyle(e).borderTopLeftRadius),
        innerShadows: [chat, panel].map(e => getComputedStyle(e).boxShadow),
        parentsUnified: shell.contains(chat) && shell.contains(panel),
        heightDifference: Math.abs(chat.getBoundingClientRect().height - panel.getBoundingClientRect().height - document.querySelector('.sidebar-tabs').getBoundingClientRect().height),
        headerHeights: [header, noticeHeader].map(e => e.getBoundingClientRect().height),
        overflows: [...document.querySelectorAll('.notice-filters,.notice-footer,.composer-footer,.tools')].filter(e => e.scrollWidth > e.clientWidth + 1).length,
        inputHeight: panel.querySelector('textarea')?.getBoundingClientRect().height,
        badLabels: [...document.querySelectorAll('.lighthouse label[for]')].filter(e => !document.getElementById(e.htmlFor)).length };
    });
    assert.deepEqual(metrics.radii, ['18px', '0px', '0px']);
    assert.deepEqual(metrics.innerShadows, ['none', 'none']);
    assert.equal(metrics.parentsUnified, true);
    assert.ok(metrics.heightDifference <= 1);
    assert.equal(metrics.overflows, 0, `controls overflow at ${width}`);
    assert.equal(metrics.badLabels, 0);
    assert.ok(metrics.inputHeight >= 34 && metrics.inputHeight <= 110);
    assert.ok(metrics.headerHeights.every(height => height >= 68));
    if (width >= 1366) {
      const rowHeight = await page.locator('.notice-row:not(.selected)').first().evaluate(el => el.getBoundingClientRect().height);
      assert.ok(rowHeight < 80, `compact notice row: ${rowHeight}`);
    }
    await page.screenshot({ path: path.join(output, `edit-${width}.png`) });
  }
  const contrast = await page.evaluate(() => {
    const root = document.querySelector('.lighthouse'), prior = root.style.getPropertyValue('--bot-color');
    const canvas = document.createElement('canvas'); canvas.width = canvas.height = 1;
    const context = canvas.getContext('2d');
    const luminance = color => { context.clearRect(0, 0, 1, 1); context.fillStyle = color; context.fillRect(0, 0, 1, 1); const rgb = [...context.getImageData(0, 0, 1, 1).data].slice(0, 3).map(c => { const s = c / 255; return s <= .04045 ? s / 12.92 : ((s + .055) / 1.055) ** 2.4; }); return .2126 * rgb[0] + .7152 * rgb[1] + .0722 * rgb[2]; };
    const ratio = (a, b) => { const aa = luminance(a), bb = luminance(b); return (Math.max(aa, bb) + .05) / (Math.min(aa, bb) + .05); };
    const results = ['#0a0a0c', '#ffffff', '#e84fa8', '#2563eb'].map(color => {
      root.style.setProperty('--bot-color', color);
      const panel = getComputedStyle(document.querySelector('.notice-panel'));
      // Read tokens without transition interpolation, matching the settled controls.
      return { color, text: ratio(panel.color, panel.backgroundColor), action: ratio('#ffffff', panel.getPropertyValue('--lh-action')) };
    });
    root.style.setProperty('--bot-color', prior); return results;
  });
  for (const item of contrast) { assert.ok(item.text >= 4.5, JSON.stringify(item)); assert.ok(item.action >= 4.5, JSON.stringify(item)); }
  await sop.selectOption('sop-one');
  sopVersion = 4;
  await page.getByRole('button', { name: '重新加载目录', exact: true }).click();
  await page.getByText('正在读取工单和人员…', { exact: true }).waitFor();
  await page.getByRole('button', { name: '采用当前版本', exact: true }).click();
  await page.getByRole('button', { name: '采用当前版本', exact: true }).waitFor({ state: 'hidden' });
  sopFail = true;
  await page.getByRole('button', { name: '重新加载目录', exact: true }).click();
  await page.getByText('隔离测试：工单目录暂不可用', { exact: true }).waitFor();
  assert.equal(await sop.inputValue(), 'sop-one', 'failed refresh keeps current selection');
  await page.getByRole('button', { name: '重新加载目录', exact: true }).click();
  await page.getByText('隔离测试：工单目录暂不可用', { exact: true }).waitFor({ state: 'hidden' });
  day = '2026-10-10'; edition = 'morning';
  await page.evaluate(() => document.dispatchEvent(new Event('visibilitychange')));
  await page.waitForFunction(() => document.querySelector('.notice-head').textContent.includes('2026-10-10 08:00'));
  await visible('.notice-row');
  assert.equal(await page.getByRole('tab', { name: '未开始计划', exact: true }).getAttribute('aria-selected'), 'true', '08:00 defaults to planned page');
  assert.equal(confirmed, 1, 'period changes never send business notices');

  // End-progress default round-trip on a native maintenance ongoing row.
  await page.getByRole('tab', { name: '进行中通告', exact: true }).click();
  await page.getByText('E楼回风机组月度维保', { exact: true }).waitFor();
  await page.getByRole('checkbox', { name: '选择E楼回风机组月度维保', exact: true }).check();
  const ongoingRow = page.locator('.notice-row').filter({ hasText: 'E楼回风机组月度维保' });
  await ongoingRow.getByLabel('本次进度', { exact: false }).waitFor();
  assert.equal(await ongoingRow.getByLabel('本次进度', { exact: false }).inputValue(), '', 'ongoing maintenance starts blank');
  await ongoingRow.getByLabel(/^本次操作/).selectOption('结束');
  assert.equal(await ongoingRow.getByLabel('本次进度', { exact: false }).inputValue(), '工作已完成，设备运行正常，请知晓！', 'selecting 结束 autofills the canned default');
  assert.equal(await ongoingRow.getByLabel('本次进度', { exact: false }).count(), 1, 'autofilled default stays visible without edit_all');
  await ongoingRow.getByLabel(/^本次操作/).selectOption('更新');
  assert.equal(await ongoingRow.getByLabel('本次进度', { exact: false }).inputValue(), '', 'back to 更新 clears the untouched default');
  await ongoingRow.getByLabel('本次进度', { exact: false }).fill('客户验收通过');
  await ongoingRow.getByLabel(/^本次操作/).selectOption('结束');
  assert.equal(await ongoingRow.getByLabel('本次进度', { exact: false }).inputValue(), '客户验收通过', 'explicit custom survives selecting 结束');
  await ongoingRow.getByLabel(/^本次操作/).selectOption('更新');
  assert.equal(await ongoingRow.getByLabel('本次进度', { exact: false }).inputValue(), '客户验收通过', 'custom is never cleared on 更新');
  await ongoingRow.getByLabel(/^本次操作/).selectOption('结束');
  assert.equal(await ongoingRow.getByLabel('本次进度', { exact: false }).inputValue(), '客户验收通过', 'custom stays put when re-entering 结束');
  await page.getByRole('button', { name: '保存草稿', exact: true }).click();
  await page.waitForFunction(() => !document.querySelector('.notice-panel[aria-busy="true"]'));
  const endDraft = docs.get(`${day}-E-morning`).items.find(row => row.key === 'n12').draft;
  assert.equal(endDraft.progress, '客户验收通过', 'saved draft keeps the custom progress');
  await page.getByLabel('楼栋', { exact: true }).selectOption('A');
  await visible('.notice-row');
  await page.getByLabel('楼栋', { exact: true }).selectOption('E');
  await page.getByRole('tab', { name: '进行中通告', exact: true }).click();
  await page.getByText('E楼回风机组月度维保', { exact: true }).waitFor();
  await page.getByRole('checkbox', { name: '选择E楼回风机组月度维保', exact: true }).check();
  const reloadRow = page.locator('.notice-row').filter({ hasText: 'E楼回风机组月度维保' });
  await reloadRow.getByLabel('本次进度', { exact: false }).waitFor();
  assert.equal(await reloadRow.getByLabel('本次进度', { exact: false }).inputValue(), '客户验收通过', 'reload keeps the custom progress visible without edit_all');

  assert.deepEqual(errors, []);
  assert.ok(polls < 30, 'bounded polling');
  console.log(JSON.stringify({ ok: true, production: true, confirmed, polls, sopReads, screenshots: output, checks: 'separate planned/ongoing pages, no hidden selection submitted, retained drafts, local switching, datetime pickers/no timezone shift, SOP/people/version, multi preview, explicit confirm, 08/17 editions, conflicts, 3 PC widths, no model' }));
} catch (error) { await page.screenshot({ path: path.join(output, 'failure.png') }); throw error; }
finally { await browser.close(); await new Promise(resolve => server.httpServer.close(resolve)); }
