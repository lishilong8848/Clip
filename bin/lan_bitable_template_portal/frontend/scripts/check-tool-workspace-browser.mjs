import assert from 'node:assert/strict';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'node:http';
import { chromium } from 'playwright';

// Only this loopback HTTP server handles API traffic; no production backend is used.
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, process.env.TOOL_WORKSPACE_FIXTURE_DIR || '../../../output/tool-workspace-preview');
const dist = path.resolve(root, process.env.TOOL_WORKSPACE_LEGACY_DIST || '../../../output/tool-workspace-build');
const v2dist = path.resolve(root, process.env.TOOL_WORKSPACE_V2_DIST || '../../../output/tool-management-v2');
const api = '/api/tool-self-check/workspace';
const layouts = {};
for (const name of ['electrical', 'hvac']) {
  layouts[name] = JSON.parse((await readFile(path.join(output, name + '.json'), 'utf8')).replace(/^\uFEFF/, ''));
  assert(layouts[name].sheets.length > 1 && layouts[name].schema.review_row, 'Use original workbook fixtures with covers and review rows');
}
const clone = value => structuredClone(value);
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const templates = [{ id: 'electrical', name: '电气原表模板' }, { id: 'hvac', name: '暖通原表模板' }];
const initialAssets = [
  { id: 'e', name: '电能质量分析仪', scope: 'A', model: 'E-100', brand: '电气品牌', profession: '电气', self_cycle: '月度', template_id: 'electrical', template_name: '电气原表模板.xlsx' },
  { id: 'h', name: '温湿度仪', scope: 'B', model: 'H-200', brand: '暖通品牌', profession: '暖通', self_cycle: '年度', template_id: 'hvac', template_name: '暖通原表模板.xlsx' },
  { id: 'u', name: '未关联工具', scope: 'A', model: 'U-1', brand: '测试品牌', self_cycle: '月度' },
  { id: 'x', name: '周期冲突工具', scope: 'A', self_cycle: '月度', external_cycle: '年度', eligible_kinds: [], disabled_reason: '同时配置了维护和送检周期，请核对。' },
  { id: 'r1', name: '送检压力表', scope: 'A', model: 'P-100', brand: '计量品牌', external_cycle: '年度' },
  { id: 'r2', name: '送检绝缘表', scope: 'A', model: 'R-100', brand: '计量品牌', external_cycle: '年度' },
];
let state;
const inventoryConflict = '组织架构表中 A 楼的楼长配置冲突，请核对楼长人员字段。';
function reset() {
  state = { assets: clone(initialAssets), sessions: new Map(), operations: new Map(), calls: [],
    failOpen: false, failSave: false, failReport: false, failPoll: false, failResume: false, warning: '', polls: 0,
    inventoryError: '', failRefresh: false, followups: [] };
}
reset();
function board() {
  return { assets: state.assets, templates, warning: state.warning, inventory_error: state.inventoryError,
    monthly_reset_at: state.monthlyReset,
    sessions: [...state.sessions.values()].map(({ items, ...rest }) => ({
      ...rest, item_count: items.length, history_items: items.map(item => ({
        id: item.id, order_id: item.order_id, asset_name: item.asset.name, scope: item.asset.scope, profession: item.asset.profession, archive_status: item.archive_status,
      })).filter(item => !state.removedOrder || item.order_id !== state.removedOrder),
    })) };
}
function createSession(body) {
  const id = 'fixture-' + (state.sessions.size + 1);
  const items = body.asset_ids.map(asset_id => {
    const asset = state.assets.find(asset => asset.id === asset_id);
    assert(asset && asset.id !== 'x');
    const template_id = body.template_ids?.[asset_id] || body.template_id || asset.template_id;
    const template = body.kind === 'self' ? clone(layouts[template_id]) : null;
    const hvac = template?.schema.layout === 'hvac';
    const values = template ? Object.fromEntries([
      [template.schema.sheet + '!' + (hvac ? 'C5' : 'C3'), asset.scope + '楼'],
      [template.schema.sheet + '!' + (hvac ? 'G6' : 'G4'), '本地测试人员'],
      [template.schema.sheet + '!' + (hvac ? 'G11' : 'G8'), '2026-11-12'],
    ]) : {};
    return { id: asset_id, asset_id, asset: clone(asset), template_id, template, values, checks: {},
      reviewer: '', reviewed_at: '', signature: '', qualified: null, can_edit: true, order_revision: 1,
      archive_status: 'draft', archive_error: '' };
  });
  const value = { id, kind: body.kind, status: 'draft', revision: 0, can_edit: true, created_at: '2026-11-12T09:00:00', items, report_name: '' };
  state.sessions.set(id, value);
  state.operations.set(body.operation_id, id);
  return value;
}
const serverErrors = [];
const server = createServer(async (request, response) => {
  const url = new URL(request.url, 'http://localhost');
  const pathname = url.pathname;
  const ok = data => {
    if (url.searchParams.get('compact') === '1' && Array.isArray(data.items) && data.items.some(item => item.template)) {
      const layouts = {};
      const items = data.items.map(({template, ...item}) => {
        if (!template) return item;
        layouts[item.template_id] = template;
        return { ...item, layout_key: item.template_id };
      });
      data = { ...data, layouts, items };
    }
    response.setHeader('Content-Type', 'application/json');
    response.end(JSON.stringify({ ok: true, data }));
  };
  const fail = (message, status = 409) => { response.writeHead(status, { 'Content-Type': 'application/json' }); response.end(JSON.stringify({ ok: false, error: message })); };
  try {
    if (pathname.startsWith('/api/')) {
      const chunks = [];
      for await (const chunk of request) chunks.push(chunk);
      const raw = Buffer.concat(chunks);
      const body = raw.length && !pathname.endsWith('/report') ? JSON.parse(raw.toString()) : null;
      state.calls.push({ method: request.method, path: pathname, body, bytes: raw.length });
      if (pathname === '/api/auth/status') return ok({ logged_in: true,
        user: { open_id: 'fixture-tools', name: '本地测试人员', role: 'admin', level: 'L0', category: 'engineer', tool_management_enabled: true, modules: ['tool_management'] },
        allowed_scopes: ['A', 'B'], scope_options: [{ value: 'A', label: 'A楼' }, { value: 'B', label: 'B楼' }], default_scope: 'A' });
      if (pathname === '/api/health') { response.setHeader('Content-Type', 'application/json'); return response.end(JSON.stringify({ ok: true, service: 'clipflow_backend', instance_id: 'tool-workspace-fixture' })); }
      if (pathname === '/api/tool-self-check') return ok({ month: '2026-11', orders: [], followups: state.followups, templates: [], can_manage_templates: true, can_create_orders: true, can_download_month: true, actor_name: '测试人员', inventory_error: state.inventoryError });
      if (pathname === '/api/tool-self-check/inventory') return ok({ items: state.assets.map(asset => ({
        ...asset, revision: asset.revision || 0, source_self_cycle: asset.self_cycle, source_external_cycle: asset.external_cycle,
      })), scrap_approvers: [] });
      if (pathname === '/api/tool-self-check/inventory/people') return ok({items:[
        {id:'custodian-a',name:'A楼工具员',category:'operator',final_scopes:['A']},
        {id:'custodian-b',name:'B楼工具员',category:'operator',final_scopes:['B']},
      ]});
      if (pathname === '/api/tool-self-check/inventory/e' && request.method === 'POST') {
        const asset = state.assets.find(row => row.id === 'e');
        assert.deepEqual(body, {responsible_id:'custodian-a',revision:asset.revision || 0});
        Object.assign(asset, {responsible_id:body.responsible_id,responsible_name:'A楼工具员',revision:body.revision+1});
        return ok({saved:true});
      }
      if (pathname.endsWith('/remove-followup')) {
        const order = state.followups.find(row => pathname.includes(row.id));
        assert(order?.can_remove_followup && body.revision === order.revision);
        order.deletion_pending = true;
        return ok({ deleted: false, deletion_pending: true });
      }
      if (pathname === api && request.method === 'GET') return ok(board());
      if (pathname === api + '/refresh') {
        assert.equal(request.method, 'POST'); assert.equal(raw.length, 0);
        if (state.failRefresh) return fail(inventoryConflict, 503);
        state.inventoryError = '';
        state.warning = '模板读取暂时不可用，已显示上次记录。';
        state.assets[0].brand = '刷新后的品牌';
        return ok(board());
      }
      if (pathname === api + '/history') {
        if (state.historyDelay) await delay(state.historyDelay);
        return ok({ sessions: board().sessions, history_cursor: null });
      }
      if (pathname === api + '/open') {
        assert.equal(request.method, 'POST'); assert(body.operation_id); assert(body.asset_ids.length);
        assert(!('template_id' in body) && !('template_ids' in body));
        await delay(180);
        const session = state.sessions.get(state.operations.get(body.operation_id)) || createSession(body);
        if (state.failOpen) { state.failOpen = false; session.status = 'opening'; session.can_edit = false; return fail('模板关联中断，请继续打开。', 503); }
        session.status = 'draft'; session.can_edit = true;
        for (const item of session.items) if (item.template_id) state.assets.find(asset => asset.id === item.id).template_id = item.template_id;
        return ok(session);
      }
      if (pathname.startsWith(api + '/')) {
        const [id, action] = pathname.slice(api.length + 1).split('/');
        const session = state.sessions.get(id);
        if (!session) return fail('记录不存在。', 404);
        if (action === 'people') return ok({ A: [{ id: 'leaderA', name: 'A楼长' }], B: [{ id: 'leaderB', name: 'B楼长' }] });
        if (action === 'resume') {
          assert.equal(request.method, 'POST'); assert.equal(raw.length, 0);
          if (state.failResume) { state.failResume = false; return fail('继续打开暂时失败。', 503); }
          session.status = 'draft'; session.can_edit = true; return ok(session);
        }
        if (action === 'save') {
          await delay(220);
          if (state.failSave) { state.failSave = false; return fail('模拟保存失败，填写内容保留。'); }
          assert.equal(body.revision, session.revision);
          for (const patch of body.items) {
            assert.equal(patch.id, patch.asset_id);
            assert(!('reviewer' in patch) && !('signature' in patch));
            const entry = session.items.find(item => item.id === patch.asset_id); assert(entry);
            const sheet = entry.template.sheets.find(sheet => sheet.name === entry.template.schema.sheet);
            const allowed = flag => new Set(sheet.rows.flatMap(row => row.cells.filter(cell => cell[flag]).map(cell => sheet.name + '!' + cell.address)));
            const allowedValues = new Set(entry.template.sheets.flatMap(tab => [
              ...tab.rows.flatMap(row => row.cells.filter(cell => cell.text_editable || cell.editable && !cell.check)
                .map(cell => tab.name + '!' + cell.address)),
              ...(tab.titles || []).map(title => tab.name + '!' + title.key),
            ]));
            for (const key of Object.keys(patch.values)) assert(allowedValues.has(key), key);
            for (const [key, value] of Object.entries(patch.checks)) { assert(allowed('check').has(key), key); assert(['是', '否'].includes(value)); }
            Object.assign(entry, patch);
          }
          session.revision++; return ok(session);
        }
        if (action === 'report') {
          assert.equal(request.method, 'POST'); assert.equal(Number(url.searchParams.get('revision')), session.revision);
          assert(raw.length > 0);
          if (state.failReport) { state.failReport = false; return fail('模拟报告上传失败。', 503); }
          session.report_name = url.searchParams.get('name'); session.revision++; return ok(session);
        }
        if (action === 'submit') {
          assert.equal(body.revision, session.revision);
          const submitted = session.items.filter(entry => !body.resubmit_asset_id || entry.id === body.resubmit_asset_id);
          if (session.kind === 'self') for (const entry of submitted) {
            assert.equal(entry.outcome, 'normal');
            assert.equal(Object.keys(entry.checks).length, entry.template.schema.checks.length);
            Object.assign(entry, {can_edit: false, can_review: true, order_revision: 2, archive_status: 'review_pending'});
          }
          else { assert(session.report_name && body.actual); session.items.forEach(entry => { entry.can_edit = false; }); }
          session.status = session.kind === 'self' ? 'review_pending' : 'pending';
          session.can_edit = session.items.some(entry => entry.can_edit);
          if (session.can_edit) session.status = 'draft';
          session.revision++; state.polls = 0; return ok(session);
        }
        if (action === 'review') {
          assert.equal(body.action, 'approve');
          assert(body.signature.startsWith('data:image/png;base64,'));
          for (const input of body.items) {
            const entry = session.items.find(item => item.id === input.asset_id); assert(entry?.can_review);
            assert.equal(input.revision, entry.order_revision);
            Object.assign(entry,{can_review:false, reviewer:'本楼楼长', reviewed_at:input.reviewed_at || body.reviewed_at || '2026-11-12T10:30',
              signature:input.signature || body.signature, order_revision:3, archive_status:'pending'});
          }
          session.status = 'pending'; session.revision++; return ok(session);
        }
        if (action === 'download') {
          assert.equal(session.status, 'archived');
          response.writeHead(200, { 'Content-Type': 'application/zip', 'Content-Disposition': 'attachment; filename="fixture-archive.zip"' });
          return response.end(Buffer.from('504b0506000000000000000000000000000000000000', 'hex'));
        }
        if (action === 'status' && request.method === 'GET') {
          if (session.status === 'pending') {
            if (state.failPoll) { state.failPoll = false; return fail('归档状态暂时无法读取。', 503); }
            state.polls++;
            session.items.forEach(entry => { entry.archive_status = state.polls < 2 ? 'verifying' : 'archived'; });
            if (state.polls >= 2) session.status = 'archived';
          }
          return ok({...session, items:session.items.map(({id, order_revision, archive_status, archive_error}) =>
            ({id, order_revision, archive_status, archive_error}))});
        }
        if (!action && request.method === 'GET') return ok(session);
      }
      if (request.method !== 'GET') throw new Error('Unexpected write: ' + pathname);
      return ok({ items: [] });
    }
    const name = pathname.startsWith('/assets/') ? pathname.slice(1) : 'index.html';
    const v2Prefix = 'assets/workspace-v2/';
    const selectedDist = name.startsWith(v2Prefix) ? v2dist : dist;
    const file = path.resolve(selectedDist, selectedDist === v2dist ? name.slice(v2Prefix.length) : name);
    assert(file.startsWith(selectedDist + path.sep));
    response.setHeader('Content-Type', ({ '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json' })[path.extname(file)] || 'application/octet-stream');
    response.setHeader('Content-Security-Policy', "connect-src 'self'; object-src 'none'");
    response.end(await readFile(file));
  } catch (error) {
    serverErrors.push(error.message);
    if (!response.headersSent) fail(error.message, 500); else response.end();
  }
});
await new Promise(resolve => server.listen(Number(process.env.TOOL_WORKSPACE_PREVIEW_PORT || 0), '127.0.0.1', resolve));
const origin = 'http://127.0.0.1:' + server.address().port;
const legacy = origin + '/risk-inspections?frontend=legacy&view=tool-self-check';
const v2 = origin + '/assets/workspace-v2/index.html?__page=/risk-inspections&view=tool-self-check';
const results = [], errors = [], blocked = [];
let browser, page, verified = false;
const button = name => page.getByRole('button', { name, exact: true });
const radio = name => page.getByRole('radio', { name, exact: true });
const chooseRadio = name => radio(name).locator('xpath=ancestor::label[1]').click();
const work = () => page.getByRole('region', { name: '工具工作台', exact: true });
const count = action => state.calls.filter(call => call.path === api + action).length;
const actions = action => state.calls.filter(call => call.path.endsWith('/' + action));
const check = async (name, run) => { await run(); results.push(name); console.log('PASS ' + name); };
const clickTwice = locator => locator.evaluate(element => { element.click(); element.click(); });
async function chooseAssets(...names) { for (const name of names) await page.getByRole('checkbox', { name: '选择工具 ' + name, exact: true }).click(); }
async function chooseSaved(text) {
  await page.locator('.opening-panel .n-select').click();
  await page.locator('.n-base-select-option').filter({ hasText: text }).first().click();
}
async function drawSignature(openName) {
  await button(openName).click();
  const canvas = page.getByLabel('手绘审核签名画板');
  await canvas.waitFor();
  await delay(350); // Modal entry resets the canvas.
  assert(await button('使用签名').isDisabled());
  const box = await canvas.boundingBox();
  await page.mouse.move(box.x + 35, box.y + 40);
  await page.mouse.down();
  await page.mouse.up();
  assert(await button('使用签名').isDisabled(), 'A click without ink is not a signature');
  await page.mouse.move(box.x + 35, box.y + 40);
  await page.mouse.down();
  await page.mouse.move(box.x + 120, box.y + 80, { steps: 8 });
  await page.mouse.move(box.x + 190, box.y + 36, { steps: 8 });
  await page.mouse.up();
  await button('使用签名').click();
  await canvas.waitFor({ state: 'hidden' });
}
async function fillChecks() { for (const select of await page.locator('.sheet-table select[aria-label^="检查 "]').all()) await select.selectOption('是'); }
async function checkWorksheetLayout() {
  const layout = await page.evaluate(() => {
    const choices = [...document.querySelectorAll('.qualified-options label')].map(label => label.getBoundingClientRect().toJSON());
    const panel = document.querySelector('.editor-panel').getBoundingClientRect().toJSON();
    return { choices, panel, radii: [...document.querySelectorAll('.sheet-table input, .sheet-table select')].map(element => getComputedStyle(element).borderRadius) };
  });
  assert.equal(layout.choices.length, 3);
  assert(Math.abs(layout.choices[0].top - layout.choices[1].top) < 2, 'Qualified choices must stay in one row');
  assert(layout.choices.every(choice => choice.bottom <= layout.panel.bottom), 'Choices stay inside editor');
  assert(layout.radii.every(radius => radius === '2px'), 'Original worksheet inputs retain compact corners');
}
async function noOverflow(panelSelector = '.tool-workspace') {
  const metrics = await page.evaluate(selector => {
    const panel = document.querySelector(selector);
    const result = { viewport: innerWidth, page: document.documentElement.scrollWidth, panel: panel.getBoundingClientRect().toJSON() };
    if (result.panel.right > innerWidth + 1 || result.page > innerWidth + 1) {
      result.ancestors = [];
      for (let node = panel; node; node = node.parentElement) {
        result.ancestors.push({ class: node.className, width: node.getBoundingClientRect().width });
      }
    }
    return result;
  }, panelSelector);
  assert(metrics.page <= metrics.viewport + 1, JSON.stringify(metrics));
  assert(metrics.panel.right <= metrics.viewport + 1, JSON.stringify(metrics));
  return metrics;
}
async function submit(label = '提交审核') { await button(label).click(); await clickTwice(button('确认提交')); }
async function openWorkspace(label = '打开填写') { await clickTwice(button(label)); }
async function checkConfigurations() {
  await check('profession filters and editable custodians on desktop and mobile', async () => {
    reset();
    await page.setViewportSize({width:1366,height:850});
    await page.goto(legacy);
    await page.locator('.assets tr').filter({hasText:'电能质量分析仪'}).waitFor();
    assert.equal(await button('工具与模板').count(),0);
    await page.locator('[aria-label="筛选工具专业"]').click();
    await page.getByText('暖通',{exact:true}).click();
    await page.locator('.assets tr').filter({hasText:'温湿度仪'}).waitFor();
    assert.equal(await page.locator('.assets tr').filter({hasText:'电能质量分析仪'}).count(),0);

    await page.goto(v2);
    const filter = page.getByRole('combobox', {name:'筛选工具专业'});
    const inventory = page.getByRole('region', {name:'筛选后的工具'});
    await inventory.getByText('电能质量分析仪', {exact:true}).waitFor();
    assert.equal(await button('工具与模板').count(),0);
    await filter.selectOption('暖通');
    assert.equal(await inventory.getByText('电能质量分析仪', {exact:true}).count(),0);
    await inventory.getByText('温湿度仪', {exact:true}).waitFor();
    await filter.selectOption('未填写');
    await inventory.getByText('未关联工具', {exact:true}).waitFor();
    await filter.selectOption('电气');
    await inventory.getByText('电能质量分析仪', {exact:true}).waitFor();

    await page.setViewportSize({width:390,height:850});
    await button('责任人配置').click();
    const modal = page.getByRole('dialog', {name:'责任人配置'});
    await modal.getByRole('button', {name:/电能质量分析仪/}).click();
    await modal.locator('.td-field').filter({hasText:'工具负责人'}).locator('input').click();
    await page.getByText('A楼工具员', {exact:true}).waitFor();
    assert.equal(await page.getByText('B楼工具员', {exact:true}).count(),0);
    await page.getByText('A楼工具员', {exact:true}).click();
    await modal.getByRole('button', {name:'保存配置',exact:true}).click();
    await modal.getByText('配置已保存。', {exact:true}).waitFor();
    const save = modal.getByRole('button', {name:'保存配置', exact:true});
    await save.waitFor();
    assert.equal((await save.textContent()).trim(), '保存配置');
    await noOverflow('.td-dialog');
    await page.screenshot({path:path.join(output,'tool-responsibility-390.png'),fullPage:true,animations:'disabled'});
    await modal.getByRole('button',{name:'关闭弹窗',exact:true}).click();
    await inventory.getByText('负责人 A楼工具员',{exact:true}).waitFor();
    assert.equal(state.calls.filter(call=>call.path==='/api/tool-self-check/inventory/e').length,1);

    createSession({kind:'self',asset_ids:['e','h'],operation_id:'history-profession'});
    state.followups = [{id:'followup-h',scope:'B',kind:'self',profession:'暖通',tool_name:'暖通检修工具',outcome:'repair'},
      {id:'followup-e',scope:'A',kind:'self',profession:'电气',tool_name:'电气检修工具',outcome:'repair'}];
    await page.getByRole('navigation',{name:'工具管理分区'}).getByRole('button',{name:'办理记录',exact:true}).click();
    await page.getByRole('combobox',{name:'筛选检修与报废专业'}).selectOption('电气');
    await page.getByText('电气检修工具',{exact:true}).waitFor();
    assert.equal(await page.getByText('暖通检修工具',{exact:true}).count(),0);
    await page.getByRole('combobox',{name:'筛选记录专业'}).selectOption('暖通');
    await page.getByText('温湿度仪',{exact:true}).waitFor();
    assert.equal(await page.getByText('电能质量分析仪',{exact:true}).count(),0);
  });
}
async function checkExistingWorkspace() {
  reset();
  await page.setViewportSize({width:1440,height:1000});
  await page.goto(legacy);
  await page.getByRole('heading', { name: '工具台账' }).waitFor();
  await check('initial GET, explicit POST refresh, visible warning and refreshed inventory', async () => {
    assert.equal(count(''), 1);
    await button('刷新').click();
    await page.getByText('刷新后的品牌', { exact: true }).waitFor();
    await page.getByText('模板读取暂时不可用，已显示上次记录。', { exact: true }).waitFor();
    assert.equal(count('/refresh'), 1);
    assert.equal(state.calls.find(call => call.path === api + '/refresh').bytes, 0);
  });
  await check('legacy selects tools directly and displays the attached workbook and profession', async () => {
    const unavailable = page.getByRole('checkbox', { name: '选择工具 周期冲突工具', exact: true });
    assert.equal(await unavailable.getAttribute('aria-checked'), 'false');
    assert.equal(await button('月度工单').count(), 0);
    await page.getByText('电能质量分析仪', { exact: true }).click();
    assert(await page.getByRole('checkbox', { name: '选择工具 电能质量分析仪', exact: true }).isChecked());
    await chooseAssets('温湿度仪');
    await page.getByText('本次 2 台', { exact: true }).waitFor();
    const review = page.getByRole('list', { name: '本次工具和模板', exact: true });
    assert(await review.getByRole('listitem').filter({ hasText: '电能质量分析仪' }).getByText('电气原表模板.xlsx').isVisible());
    assert(await review.getByRole('listitem').filter({ hasText: '温湿度仪' }).getByText('暖通原表模板.xlsx').isVisible());
    assert.match(await page.locator('.assets tr').filter({ hasText: '温湿度仪' }).innerText(), /专业 暖通/);
    assert.equal(await page.getByRole('heading', { name: '自检模板库', exact: true }).count(), 0);
    assert.equal(await review.getByRole('listitem').count(), 2);
    await noOverflow();
    await page.screenshot({ path: path.join(output, 'desktop-selection.png'), fullPage: true });
    await button('取消全部').click();
    assert.equal(await review.count(), 0);
    assert(await button('打开填写').isDisabled());
    await chooseAssets('电能质量分析仪');
    await chooseAssets('温湿度仪');
  });
  await check('repeat open is guarded; partial opening survives reload; resume is bodyless and retryable', async () => {
    state.failOpen = true;
    await openWorkspace();
    await page.getByText('模板关联中断，请继续打开。', { exact: true }).waitFor();
    assert.equal(count('/open'), 1); assert.equal(state.sessions.size, 1);
    const operation = actions('open')[0].body.operation_id;
    assert.deepEqual(actions('open')[0].body.asset_ids, ['e', 'h']);
    assert.deepEqual([...state.sessions.values()][0].items.map(item => item.template_id), ['electrical', 'hvac']);
    state.failOpen = true;
    await openWorkspace();
    await page.getByText('模板关联中断，请继续打开。', { exact: true }).waitFor();
    assert.equal(actions('open')[1].body.operation_id, operation); assert.equal(state.sessions.size, 1);
    await page.reload();
    state.failResume = true;
    await chooseSaved('模板关联待核对');
    await page.getByText('继续打开暂时失败。', { exact: true }).waitFor();
    await chooseSaved('模板关联待核对');
    await page.getByRole('heading', { name: '自检记录', exact: true }).waitFor();
    assert.equal(actions('resume').length, 2); assert.equal(actions('resume')[0].bytes, 0);
    assert.equal(await page.locator('.workspace-grid').count(), 0);
  });
  await check('real electrical grid, original cover/logo, merged cells and cleared check draft', async () => {
    assert.equal(await page.locator('.sheet-table tbody tr').count(), layouts.electrical.sheets[1].rows.length);
    assert(await page.locator('.sheet-table td[colspan="2"]').count() > 0);
    await button('封面').click();
    assert.equal(await page.locator('.sheet-frame > img').count(), layouts.electrical.sheets[0].images.length);
    await button(layouts.electrical.schema.sheet).click();
    const titleKey = layouts.electrical.schema.sheet + '!' + layouts.electrical.sheets[1].titles[0].key;
    await button('编辑表格标题').click();
    await page.getByRole('textbox',{name:'填写表格标题'}).fill('逐台复核标题');
    const professional = layouts.electrical.schema.sheet + '!G3';
    const professionInput = page.getByRole('textbox',{name:'填写 ' + professional, exact:true});
    assert.equal(await professionInput.inputValue(),'电气');
    await professionInput.fill('电气检修');
    await page.getByLabel('填写 ' + layouts.electrical.schema.sheet + '!C6', { exact: true }).fill('草稿保留内容');
    const select = page.locator('.sheet-table select').first();
    await select.selectOption('是'); await select.selectOption('');
    await button('保存草稿').click();
    await page.getByText('草稿已保存', { exact: true }).waitFor();
    assert.deepEqual(actions('save').at(-1).body.items[0].checks, {});
    assert.equal(actions('save').at(-1).body.items[0].values[titleKey], '逐台复核标题');
    assert.equal(actions('save').at(-1).body.items[0].values[professional], '电气检修');
  });
  await check('failed save keeps all edits; repeat save guarded; switching tools and canceling exit retain data', async () => {
    const note = page.getByLabel('填写 ' + layouts.electrical.schema.sheet + '!C6', { exact: true });
    await note.fill('逐台填写内容');
    state.failSave = true;
    const before = actions('save').length;
    await clickTwice(button('保存草稿'));
    await page.getByText('模拟保存失败，填写内容保留。', { exact: true }).waitFor();
    assert.equal(actions('save').length, before + 1);
    assert.equal(await note.inputValue(), '逐台填写内容');
    await button('温湿度仪 · B楼').click();
    assert.equal(await page.locator('.sheet-table tbody tr').count(), layouts.hvac.sheets[1].rows.length);
    await button('电能质量分析仪 · A楼').click();
    assert.equal(await note.inputValue(), '逐台填写内容');
    await button('返回选择').click();
    await page.getByText('放弃未保存内容？', { exact: true }).waitFor();
    await button('取消').click();
    await button('保存草稿').click();
    await page.getByText('草稿已保存', { exact: true }).waitFor();
  });
  await check('inspector/date defaults, one-click normal, and explicit completion are editable', async () => {
    assert.equal(await page.getByLabel('填写 '+layouts.electrical.schema.sheet+'!G4',{exact:true}).inputValue(),'本地测试人员');
    const checkedAt=page.getByLabel('填写 '+layouts.electrical.schema.sheet+'!G8',{exact:true});
    assert.equal(await checkedAt.getAttribute('type'),'datetime-local');
    assert.equal(await checkedAt.inputValue(),'2026-11-12T00:00');
    await button('一键正常').click();
    assert.equal(await page.getByLabel('完成情况',{exact:true}).inputValue(),'completed');
    await page.getByLabel('完成情况',{exact:true}).selectOption('incomplete');
    const quickCompletion = page.getByLabel('填写完成情况', { exact: true });
    assert.equal(await quickCompletion.inputValue(), 'incomplete');
    await quickCompletion.selectOption('completed');
    assert.equal(await page.getByLabel('完成情况', {exact:true}).inputValue(), 'completed');
    await quickCompletion.selectOption('incomplete');
    await page.getByLabel('选择审核人',{exact:true}).focus();
    await page.getByLabel('选择审核人',{exact:true}).selectOption('leaderA');
    await checkWorksheetLayout();
    assert.equal(await page.getByLabel('审核人', { exact: true }).count(), 0);
    await page.screenshot({ path: path.join(output, 'desktop-electrical-review.png'), fullPage: true });
    await button('温湿度仪 · B楼').click();
    assert.equal(await page.locator('.sheet-scroll').evaluate(element => element.scrollTop), 0, 'Changing tools resets only worksheet scroll');
    await fillChecks(); await chooseRadio('合格');
    await page.getByLabel('填写 ' + layouts.hvac.schema.sheet + '!E20', { exact: true }).fill('23.5');
    await page.locator('.sheet-table tr').last().scrollIntoViewIfNeeded();
    await page.screenshot({ path: path.join(output, 'desktop-hvac-review.png'), fullPage: true });
  });
  await check('submit stops on save failure and retains draft; polling recovers and downloads archived ZIP', async () => {
    state.failSave = true;
    await submit();
    await page.getByText('模拟保存失败，填写内容保留。', { exact: true }).waitFor();
    assert.equal(actions('submit').length, 0);
    assert.equal(await page.getByLabel('填写 ' + layouts.hvac.schema.sheet + '!E20', { exact: true }).inputValue(), '23.5');
    await submit();
    await page.setViewportSize({ width: 390, height: 844 });
    await page.getByLabel('审核时间（北京时间）', { exact: true }).fill('2026-11-12T10:25');
    assert.equal(await page.getByLabel('审核年月日时', { exact: true }).inputValue(), '2026-11-12T10:25');
    await noOverflow();
    await page.screenshot({ path: path.join(output, 'mobile-review-time.png'), fullPage: true });
    await button('批量审核').click();
    await page.locator('.tool-workspace-dialog').filter({hasText:'批量审核'})
      .getByLabel('审核时间（北京时间）', { exact: true }).fill('2026-11-12T10:30');
    await drawSignature('绘制批量签名');
    state.failPoll = true;
    await page.locator('.tool-workspace-dialog').filter({hasText:'重签批量签名'})
      .getByRole('button', {name:'审核通过', exact:true}).click();
    await page.getByText('归档处理中', { exact: true }).waitFor();
    assert.equal(actions('submit').length, 1);
    assert.equal(actions('review').length, 1);
    assert.equal(actions('review')[0].body.reviewed_at, '2026-11-12T10:30');
    assert.equal(actions('review')[0].body.items.find(item => item.asset_id === 'h').reviewed_at, '2026-11-12T10:25');
    assert.equal(actions('review')[0].body.items.find(item => item.asset_id === 'e').reviewed_at, undefined);
    await page.getByText('归档状态暂时无法读取。', { exact: true }).waitFor();
    await page.getByRole('link', { name: '下载归档', exact: true }).waitFor({ timeout: 20000 });
    const download = page.waitForEvent('download');
    await page.getByRole('link', { name: '下载归档', exact: true }).click();
    assert.equal((await download).suggestedFilename(), 'fixture-archive.zip');
    await page.setViewportSize({ width: 1440, height: 1000 });
  });
  await check('archived history reopens with original layouts, completion and review signatures', async () => {
    await button('返回选择').click();
    state.inventoryError = inventoryConflict;
    state.failRefresh = true;
    await page.reload();
    const conflict = page.getByText(inventoryConflict, { exact: true });
    await conflict.waitFor();
    await button('查看办理记录').click();
    const archivedTool = page.locator('.record-tools li').filter({ hasText: '电能质量分析仪' });
    await archivedTool.waitFor();
    await button('刷新').click();
    await page.getByRole('alert').filter({ hasText: inventoryConflict }).waitFor();
    assert.equal(await conflict.count(), 1);
    await button('刷新记录').click();
    await archivedTool.waitFor();
    state.failRefresh = false;
    await button('刷新').click();
    await conflict.waitFor({ state: 'hidden' });
    await archivedTool.getByRole('button', { name: '查看记录', exact: true }).click();
    await page.getByRole('heading', { name: '自检记录', exact: true }).waitFor();
    await button('电能质量分析仪 · A楼').click();
    await page.getByText('已完成[ ]　未完成[√]', {exact: true}).waitFor();
    assert.equal(await page.getByLabel('填写 ' + layouts.electrical.schema.sheet + '!C6', {exact: true}).inputValue(), '逐台填写内容');
    assert(await page.getByAltText('表内审核签名',{exact:true}).isVisible());
    await page.locator('.sheet-table').getByText('2026-11-12 10:30', {exact:true}).waitFor();
    await button('温湿度仪 · B楼').click();
    await page.locator('.sheet-table').getByText('2026-11-12 10:25', {exact:true}).waitFor();
    assert.equal(await page.locator('.sheet-table tbody tr').count(), layouts.hvac.sheets[1].rows.length);
    await page.setViewportSize({ width: 320, height: 700 });
    await noOverflow();
    await checkWorksheetLayout();
    const signedRow = await page.getByAltText('表内审核签名', {exact:true}).evaluate(image => {
      const row = image.closest('tr');
      const cell = image.closest('td').getBoundingClientRect();
      const signature = image.getBoundingClientRect();
      const signedHeight = row.getBoundingClientRect().height;
      image.style.display = 'none';
      const unsignedHeight = row.getBoundingClientRect().height;
      image.style.removeProperty('display');
      return { signedHeight, unsignedHeight, fits: signature.top >= cell.top && signature.bottom <= cell.bottom };
    });
    assert(signedRow.fits && signedRow.signedHeight <= signedRow.unsignedHeight + 1, JSON.stringify(signedRow));
    await page.screenshot({ path: path.join(output, 'mobile-archived-320.png'), fullPage: true });
    await page.setViewportSize({ width: 1440, height: 1000 });
    await button('返回选择').click();
    await page.getByRole('heading', { name: '办理记录', exact: true }).waitFor();
  });
  await check('external tools share one binary report; upload failure retains file, repeat upload/submit guarded', async () => {
    await button('返回选择').click();
    await chooseRadio('送检');
    await chooseAssets('送检压力表', '送检绝缘表');
    await openWorkspace('打开送检');
    await page.getByText('2 台工具共用一份送检报告。', { exact: true }).waitFor();
    await work().locator('input[type=file]').setInputFiles({ name: '共用报告.pdf', mimeType: 'application/pdf', buffer: Buffer.from('%PDF-1.4\nfixture only') });
    state.failReport = true;
    await clickTwice(button('上传报告'));
    await page.getByText('模拟报告上传失败。', { exact: true }).waitFor();
    await button('上传报告').waitFor(); assert.equal(actions('report').length, 1);
    await button('上传报告').click();
    await page.getByText('共用报告.pdf', { exact: true }).waitFor();
    assert.equal(actions('report').length, 2);
    await page.setViewportSize({ width: 390, height: 844 });
    await page.getByLabel('实际检测日期').fill('2026-11-12');
    assert.equal(await button('提交归档').isDisabled(), true);
    await page.getByLabel('报告编号').fill('TEST-REPORT');
    await page.getByLabel('检测机构').fill('测试检测机构');
    await page.getByLabel('检测结论', {exact:true}).selectOption('qualified');
    await page.getByText(/逐台检测结论/).click();
    await page.getByLabel('送检绝缘表检测结论', {exact:true}).selectOption('unqualified');
    await noOverflow();
    await page.screenshot({ path: path.join(output, 'mobile-external-report.png'), fullPage: true });
    await page.getByRole('checkbox', {name:/已核对报告/}).check();
    await submit('提交归档');
    await page.getByRole('link', { name: '下载归档', exact: true }).waitFor();
    assert.equal([...state.sessions.values()].at(-1).items.length, 2);
    assert.deepEqual(actions('submit').at(-1).body.inspection.tools.r2, {result:'unqualified', expires_on:''});
    await page.setViewportSize({ width: 1440, height: 1000 });
  });
  await check('tools without a workbook remain visible and cannot be newly filled', async () => {
    reset(); await page.goto(legacy);
    const tool = page.locator('.assets tr').filter({ hasText: '未关联工具' });
    await tool.getByText('工具台账缺少模板文件', { exact: true }).waitFor();
    assert.match(await tool.getByRole('checkbox').getAttribute('class') || '', /checkbox--disabled/);
    assert(await button('打开填写').isDisabled());
  });
  await check('V2 desktop and mobile selection/editor; multiple templates remain separate', async () => {
    reset(); await page.setViewportSize({ width: 1920, height: 1000 }); await page.goto(v2);
    await page.getByRole('heading', { name: '工具管理', exact: true }).waitFor();
    await page.getByRole('heading', { name: /^工具清单/ }).waitFor();
    await noOverflow('.td-page');
    assert(await page.locator('.pair-desk').evaluate(node => node.getBoundingClientRect().width > innerWidth * .9),
      'Tool management should use the available page width');
    assert.equal(await page.getByRole('combobox', { name: '继续已有批次' }).count(),0);
    await page.screenshot({ path: path.join(output, 'v2-desktop-selection.png'), fullPage: true });
    await page.setViewportSize({ width: 390, height: 844 });
    await noOverflow('.td-page');
    await page.screenshot({ path: path.join(output, 'mobile-selection.png'), fullPage: true });
    const row = page.locator('.pair-asset').filter({ hasText: '电能质量分析仪' });
    state.historyDelay = 5000;
    await row.getByRole('button', { name: '记录', exact: true }).click();
    await page.getByRole('heading', { name: '电能质量分析仪的办理记录' }).waitFor();
    await button('返回选取工具').click({ timeout: 1500 });
    await page.getByRole('heading', { name: /^工具清单/ }).waitFor();
    state.historyDelay = 0;
    for (const tool of ['电能质量分析仪', '温湿度仪']) {
      await page.getByRole('checkbox', { name: '选择' + tool, exact: true }).check();
    }
    assert(await page.getByRole('checkbox', { name: '选择未关联工具', exact: true }).isDisabled());
    await button('取消全部').click();
    assert.equal(await page.getByRole('list', { name: '本批工具' }).count(), 0);
    assert(await button('开始填写自检').isDisabled());
    for (const name of ['电能质量分析仪', '温湿度仪']) {
      const choice = page.getByRole('checkbox', { name: '选择' + name, exact: true });
      assert(!(await choice.isChecked()) && !(await choice.isDisabled()));
    }
    for (const [tool, template] of [['电能质量分析仪', '电气原表模板.xlsx'], ['温湿度仪', '暖通原表模板.xlsx']]) {
      await page.getByRole('checkbox', { name: '选择' + tool, exact: true }).check();
      const member = page.getByRole('list', { name: '本批工具' }).getByRole('listitem').filter({ hasText: tool });
      await member.getByText(template, {exact: false}).waitFor();
    }
    assert.equal(await page.getByRole('heading', { name: '配对模板', exact: true }).count(), 0);
    await clickTwice(button('开始填写自检'));
    await page.getByRole('heading', { name: '维护自检 · 2 台', exact: true }).waitFor();
    assert.equal(count('/open'), 1);
    assert.deepEqual(actions('open')[0].body.asset_ids, ['e', 'h']);
    await page.getByText('专业 电气 · 电气原表模板.xlsx', { exact: true }).waitFor();
    const currentTool = page.getByRole('combobox', { name: '当前工具', exact: true });
    const sheet = page.getByRole('combobox', { name: '工作表', exact: true });
    assert.equal(await sheet.inputValue(), layouts.electrical.schema.sheet);
    const cellKey = layouts.electrical.schema.sheet + '!C6';
    await page.getByRole('region', { name: '表格预览', exact: true }).waitFor();
    assert.equal(await button('字段填写').count(),0);
    const firstKey = layouts.electrical.schema.sheet + '!A1';
    await page.locator('.paper-cell[data-address="A1"]').click({ position: { x: 2, y: 2 } });
    const firstEditor = page.getByRole('dialog', { name: '填写 ' + firstKey, exact: true });
    const firstInput = firstEditor.getByRole('textbox');
    await firstInput.waitFor();
    assert(await firstInput.evaluate(node => node === document.activeElement), 'The first cell accepts typing on open');
    await page.keyboard.press('ControlOrMeta+A');
    await page.keyboard.insertText('首格草稿');
    await page.keyboard.press('Escape');
    await firstEditor.waitFor({ state: 'hidden' });
    assert.equal(await button('编辑 ' + firstKey).innerText(), '首格草稿');
    await button('编辑 ' + cellKey).click();
    const cellEditor = page.getByRole('dialog', { name: '填写 ' + cellKey, exact: true });
    const note = cellEditor.getByLabel('填写 ' + cellKey, { exact: true });
    await note.fill('手机草稿内容');
    await cellEditor.getByRole('button', { name: '关闭弹窗', exact: true }).click();
    await cellEditor.waitFor({ state: 'hidden' });
    assert.equal(await button('编辑 ' + cellKey).innerText(), '手机草稿内容');
    await noOverflow('.td-page');
    await page.screenshot({ path: path.join(output, 'mobile-editor.png'), fullPage: true });
    await page.setViewportSize({ width: 320, height: 700 });
    const completion = page.getByRole('radiogroup', { name: '原表完成情况', exact: true })
      .getByRole('radio', { name: '已完成', exact: true });
    await completion.check();
    const reviewerKey = layouts.electrical.schema.sheet + '!E' + layouts.electrical.schema.review_row;
    await button('选择审核人 ' + reviewerKey).click();
    const reviewerEditor = page.getByRole('dialog', { name: '填写 ' + reviewerKey, exact: true });
    const reviewer = reviewerEditor.getByRole('combobox', { name: '审核人', exact: true });
    await reviewer.selectOption('leaderA');
    assert.equal(await reviewer.inputValue(), 'leaderA');
    await reviewerEditor.getByRole('button', { name: '关闭弹窗', exact: true }).click();
    assert.equal(await completion.isChecked(), true);
    await noOverflow('.td-page');
    const submitV2 = button('提交自检审核');
    await submitV2.scrollIntoViewIfNeeded();
    assert(await submitV2.evaluate(node => {
      const box = node.getBoundingClientRect();
      return [.15, .5, .85].every(offset => node.contains(document.elementFromPoint(
        box.left + box.width * offset, (box.top + box.bottom) / 2)));
    }), 'Mobile submit stays reachable across the button width');
    await page.screenshot({ path: path.join(output, 'mobile-editor-320.png'), fullPage: true });
    await page.setViewportSize({ width: 390, height: 844 });
    await currentTool.selectOption('h');
    assert.equal(await sheet.inputValue(), layouts.hvac.schema.sheet);
    await currentTool.selectOption('e');
    assert.equal(await sheet.inputValue(), layouts.electrical.schema.sheet);
    assert.equal(await button('编辑 ' + cellKey).innerText(), '手机草稿内容');
    const outcome = page.getByRole('combobox', { name: '检查结果', exact: true });
    await outcome.selectOption('scrap');
    const photo = await page.evaluate(() => {
      const canvas = document.createElement('canvas');
      canvas.width = 600;
      canvas.height = 400;
      const context = canvas.getContext('2d');
      context.fillStyle = '#246';
      context.fillRect(0, 0, canvas.width, canvas.height);
      return canvas.toDataURL('image/png').split(',')[1];
    });
    await page.getByLabel(/报废现场照片（最多/).setInputFiles({
      name: '隔离测试现场照片.png', mimeType: 'image/png', buffer: Buffer.from(photo, 'base64'),
    });
    const thumbnail = page.getByRole('button', { name: '报废现场照片', exact: true });
    await thumbnail.click();
    const enlarged = page.getByRole('dialog', { name: '报废现场照片', exact: true }).getByRole('img', { name: '报废现场照片大图' });
    await enlarged.waitFor();
    await enlarged.evaluate(image => image.decode());
    const dimensions = await enlarged.boundingBox();
    assert(dimensions.width > (await thumbnail.boundingBox()).width);
    assert(dimensions.x >= 0 && dimensions.x + dimensions.width <= 390);
    await page.screenshot({ path: path.join(output, 'mobile-scrap-photo.png') });
    await page.keyboard.press('Escape');
    await enlarged.waitFor({ state: 'hidden' });
    await outcome.selectOption('normal');
    const saved = page.waitForResponse(response => new URL(response.url()).pathname.endsWith('/save') && response.request().method() === 'POST');
    await button('保存草稿').click();
    assert.equal((await saved).status(), 200);
    const items = actions('save').at(-1).body.items;
    assert.deepEqual(items.map(item => item.id), ['e']);
    assert.equal(items[0].values[firstKey], '首格草稿');
    assert.equal(items[0].values[cellKey], '手机草稿内容');
  });
  await check('V2 keeps 200 tools reachable in one selector on mobile and desktop', async () => {
    reset();
    state.assets = Array.from({ length: 200 }, (_, index) => ({ ...clone(initialAssets[0]), id: 'bulk-' + index, name: '批量工具' + (index + 1) }));
    await page.goto(v2);
    await page.getByRole('checkbox', { name: '全选可办', exact: true }).check();
    await clickTwice(button('开始填写自检'));
    await page.getByRole('heading', { name: '维护自检 · 200 台', exact: true }).waitFor({ timeout: 30000 });
    const tool = page.getByRole('combobox', { name: '当前工具', exact: true });
    assert.equal(await tool.locator('option').count(), 200);
    for (const width of [390, 1440]) {
      await page.setViewportSize({ width, height: 1000 });
      await noOverflow('.td-page');
      const target = width === 390 ? 'bulk-199' : 'bulk-0';
      await tool.selectOption(target);
      assert.equal(await tool.inputValue(), target);
      await page.screenshot({ path: path.join(output, `select-200-${width}.png`) });
    }
    assert.equal(count('/open'), 1);
    assert.equal(actions('open')[0].body.asset_ids.length, 200);
  });
  await check('one records page deduplicates repair/scrap and waits for deletion confirmation', async () => {
    for (const [location, width] of [[legacy, 1440], [v2, 390]]) {
      reset();
      const id = 'archived-followup';
      const session = createSession({ asset_ids: ['e', 'u'], template_ids: { u: 'electrical' }, kind: 'self' });
      session.items[0].order_id = id;
      state.followups = [{ id, tool_name: '电能质量分析仪', scope: 'A', kind: 'self', outcome: 'repaired', status: 'submitted',
        revision: 2, result_label: '合格', tool_status_label: '检修后合格', can_remove_followup: true, sync: [] }];
      await page.setViewportSize({ width, height: 900 });
      await page.goto(location);
      if (location === v2) {
        await button('办理记录').click();
        const order = page.locator('.tool-worklist article').filter({ hasText: '电能质量分析仪' });
        const history = page.locator('.td-records');
        await order.waitFor();
        await history.getByText('未关联工具', { exact: true }).waitFor();
        assert.equal(await history.getByText('电能质量分析仪', { exact: true }).count(), 0);
        await noOverflow('.td-page');
        await order.getByRole('button', { name: '删除记录', exact: true }).click();
        await page.locator('.n-popconfirm').getByRole('button', { name: '确认', exact: true }).click();
        await order.getByText('删除结果核验中', { exact: true }).waitFor();
        assert.equal(await order.getByRole('button', { name: '删除记录', exact: true }).count(), 0);
        state.removedOrder = id;
        state.followups = [];
        await button('刷新').click();
        await order.waitFor({ state: 'hidden' });
        await history.getByText('未关联工具', { exact: true }).waitFor();
        continue;
      }
      await page.getByRole('navigation', { name: '工具管理分区' }).getByRole('button', { name: '办理记录', exact: true }).click();
      const followups = page.getByRole('table', { name: '检修和报废记录' });
      await followups.getByText('电能质量分析仪', { exact: true }).waitFor();
      await page.locator('.record-tools').getByText('未关联工具', { exact: true }).waitFor();
      assert.equal(await page.locator('.record-tools').getByText('电能质量分析仪', { exact: true }).count(), 0);
      await noOverflow();
      await followups.getByRole('button', { name: '删除', exact: true }).click();
      await page.locator('.n-popover').getByRole('button', { name: '删除', exact: true }).click();
      await followups.getByText('等待飞书确认删除', { exact: true }).waitFor();
      await followups.getByText('电能质量分析仪', { exact: true }).waitFor();
      state.removedOrder = id;
      state.followups = [];
      await page.getByRole('button', { name: '刷新记录', exact: true }).click();
      await followups.waitFor({ state: 'hidden' });
      await page.locator('.record-tools').getByText('未关联工具', { exact: true }).waitFor();
      assert.equal(state.removedOrder, id);
    }
  });
  await check('rejection card link opens the specified editable item in both workspaces', async () => {
    for (const location of [legacy, v2]) {
      reset();
      const session = createSession({ asset_ids: ['e', 'h'], kind: 'self' });
      state.sessions.delete(session.id);
      session.id = 'a'.repeat(32);
      state.sessions.set(session.id, session);
      const target = session.items[1];
      target.id = target.asset_id = target.asset.id = 'recSecondTool';
      target.review_note = '请重新核对第二台工具';
      target.review_status = 'rejected';
      target.result_notice = '结果通知待重试 · 飞书未确认送达，将自动重试。';
      await page.goto(location + '&scope=B&tool_session=' + session.id + '&tool_asset=' + target.id);
      const isV2 = location === v2;
      const resubmit = isV2 ? '重新提交本台审核' : '重新提交本台';
      if (isV2) {
        await page.getByText(target.review_note).waitFor();
        assert.equal(await page.getByRole('combobox', { name: '当前工具', exact: true }).inputValue(), target.id);
        await page.getByText(target.result_notice).waitFor();
        await button('本表全部正常').click();
        await page.getByRole('dialog', { name: '本表检查项全部正常？' }).getByRole('button', { name: '确认', exact: true }).click();
      } else {
        await page.getByText(target.review_note, { exact: true }).waitFor();
        assert.equal(await button('温湿度仪 · B楼').getAttribute('aria-pressed'), 'true');
        await page.getByText(target.result_notice, { exact: true }).waitFor();
        await button('一键正常').click();
      }
      const [submitted] = await Promise.all([
        page.waitForResponse(r => new URL(r.url()).pathname.endsWith('/submit') && r.request().method() === 'POST'),
        isV2 ? (async () => {
          await button(resubmit).click();
          await page.getByRole('dialog', { name: resubmit + '？' }).getByRole('button', { name: '确认', exact: true }).click();
        })() : submit(resubmit),
      ]);
      assert.equal(submitted.status(), 200);
      await button(resubmit).waitFor({state:'hidden'});
      assert.equal(actions('submit').at(-1).body.resubmit_asset_id, target.id);
      assert.equal(session.items[0].can_edit, true);
    }
  });
  await check('V2 refreshes monthly tool selection at the Beijing month boundary', async () => {
    reset();
    const original = state.assets.shift();
    state.assets.push({ ...original, self_cycle: '每月', eligible_kinds: [], monthly_locked: true, disabled_reason: '本月已填写' });
    state.monthlyReset = '2026-12-01T00:00:00+08:00';
    await page.clock.install({ time: new Date('2026-11-30T23:59:55+08:00') });
    await page.goto(v2);
    await page.getByText('本月已填写', { exact: true }).waitFor();
    const tool = page.getByRole('checkbox', { name: '选择电能质量分析仪', exact: true });
    assert(await tool.isDisabled());
    assert.equal(await page.locator('.pair-asset').last().locator('strong').textContent(), original.name);
    state.assets.pop();
    state.assets.unshift(original);
    state.monthlyReset = '2027-01-01T00:00:00+08:00';
    await page.clock.fastForward(21_000);
    await tool.check({timeout: 5000});
    assert(await tool.isChecked());
  });
}
try {
  await mkdir(output, { recursive: true });
  browser = await chromium.launch();
  page = await browser.newPage({ viewport: { width: 1440, height: 1000 }, acceptDownloads: true });
  page.setDefaultTimeout(15000);
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/*', route => {
    const url = new URL(route.request().url());
    if (url.origin === origin || ['data:', 'blob:'].includes(url.protocol)) return route.continue();
    blocked.push(url.origin); return route.abort();
  });
  await checkConfigurations();
  if (!process.argv.includes('--configuration-only')) await checkExistingWorkspace();
  assert.deepEqual(errors, []); assert.deepEqual(serverErrors, []); assert.deepEqual(blocked, []);
  verified = true;
  console.log('PASS all tool workspace browser checks');
} catch (error) {
  if (page) {
    await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true }).catch(() => {});
    await writeFile(path.join(output, 'failure.txt'), await page.locator('body').innerText().catch(() => ''));
  }
  throw error;
} finally {
  await writeFile(path.join(output, 'browser-results.json'), JSON.stringify({ passed: results, errors, serverErrors, blocked, legacy, v2 }, null, 2));
  await browser?.close();
  if (process.env.TOOL_WORKSPACE_KEEP_PREVIEW === '1' && verified) {
    reset();
    await writeFile(path.join(output, 'preview-server.json'), JSON.stringify({ pid: process.pid, legacy, v2, fixturesOnly: true }, null, 2));
    console.log('Fixture preview remains available: ' + v2);
  } else await new Promise(resolve => server.close(resolve));
}
