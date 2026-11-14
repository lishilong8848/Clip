import assert from 'node:assert/strict';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'node:http';
import { chromium } from 'playwright';

// Only this loopback HTTP server handles API traffic; no production backend is used.
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/tool-workspace-preview');
const dist = path.resolve(root, '../../../output/tool-workspace-build');
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
  { id: 'e', name: '电能质量分析仪', scope: 'A', model: 'E-100', brand: '电气品牌', self_cycle: '月度', template_id: 'electrical' },
  { id: 'h', name: '温湿度仪', scope: 'B', model: 'H-200', brand: '暖通品牌', self_cycle: '年度', template_id: 'hvac' },
  { id: 'u', name: '未关联工具', scope: 'A', model: 'U-1', brand: '测试品牌', self_cycle: '月度' },
  { id: 'x', name: '周期冲突工具', scope: 'A', self_cycle: '月度', external_cycle: '年度', eligible_kinds: [], disabled_reason: '同时配置了维护和送检周期，请核对。' },
  { id: 'r1', name: '送检压力表', scope: 'A', model: 'P-100', brand: '计量品牌', external_cycle: '年度' },
  { id: 'r2', name: '送检绝缘表', scope: 'B', model: 'R-100', brand: '计量品牌', external_cycle: '年度' },
];
let state;
function reset() {
  state = { assets: clone(initialAssets), sessions: new Map(), operations: new Map(), calls: [],
    failOpen: false, failSave: false, failReport: false, failPoll: false, failResume: false, warning: '', polls: 0 };
}
reset();
function board() {
  return { assets: state.assets, templates, warning: state.warning, sessions: [...state.sessions.values()].map(({ items, ...rest }) => ({ ...rest, item_count: items.length })) };
}
function createSession(body) {
  const id = 'fixture-' + (state.sessions.size + 1);
  const items = body.asset_ids.map(asset_id => {
    const asset = state.assets.find(asset => asset.id === asset_id);
    assert(asset && asset.id !== 'x');
    const template_id = body.template_id || asset.template_id;
    const template = body.kind === 'self' ? clone(layouts[template_id]) : null;
    const values = template ? { [template.schema.sheet + '!' + (template.schema.layout === 'hvac' ? 'C5' : 'C3')]: asset.scope + '楼' } : {};
    return { id: asset_id, asset_id, asset: clone(asset), template_id, template, values, checks: {},
      reviewer: '', reviewed_at: '', signature: '', qualified: null, archive_status: 'draft', archive_error: '' };
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
  const ok = data => { response.setHeader('Content-Type', 'application/json'); response.end(JSON.stringify({ ok: true, data })); };
  const fail = (message, status = 409) => { response.writeHead(status, { 'Content-Type': 'application/json' }); response.end(JSON.stringify({ ok: false, error: message })); };
  try {
    if (pathname.startsWith('/api/')) {
      const chunks = [];
      for await (const chunk of request) chunks.push(chunk);
      const raw = Buffer.concat(chunks);
      const body = raw.length && !pathname.endsWith('/report') ? JSON.parse(raw.toString()) : null;
      state.calls.push({ method: request.method, path: pathname, body, bytes: raw.length });
      if (pathname === '/api/auth/status') return ok({ logged_in: true,
        user: { open_id: 'fixture-tools', name: '本地测试人员', role: 'admin', level: 'L0', category: 'engineer', tool_management_enabled: true, modules: ['risk_inspection'] },
        allowed_scopes: ['A', 'B'], scope_options: [{ value: 'A', label: 'A楼' }, { value: 'B', label: 'B楼' }], default_scope: 'A' });
      if (pathname === '/api/health') { response.setHeader('Content-Type', 'application/json'); return response.end(JSON.stringify({ ok: true, service: 'clipflow_backend', instance_id: 'tool-workspace-fixture' })); }
      if (pathname === '/api/tool-self-check') return ok({ month: '2026-11', orders: [], followups: [], templates: [], can_manage_templates: true, can_create_orders: true, can_download_month: true, actor_name: '测试人员' });
      if (pathname === api && request.method === 'GET') return ok(board());
      if (pathname === api + '/refresh') {
        assert.equal(request.method, 'POST'); assert.equal(raw.length, 0);
        state.warning = '模板读取暂时不可用，已显示上次记录。';
        state.assets[0].brand = '刷新后的品牌';
        return ok(board());
      }
      if (pathname === api + '/open') {
        assert.equal(request.method, 'POST'); assert(body.operation_id); assert(body.asset_ids.length);
        await delay(180);
        const session = state.sessions.get(state.operations.get(body.operation_id)) || createSession(body);
        if (state.failOpen) { state.failOpen = false; session.status = 'opening'; session.can_edit = false; return fail('模板关联中断，请继续打开。', 503); }
        session.status = 'draft'; session.can_edit = true;
        if (body.template_id) state.assets.forEach(asset => { if (body.asset_ids.includes(asset.id)) asset.template_id = body.template_id; });
        return ok(session);
      }
      if (pathname.startsWith(api + '/')) {
        const [id, action] = pathname.slice(api.length + 1).split('/');
        const session = state.sessions.get(id);
        if (!session) return fail('记录不存在。', 404);
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
            const entry = session.items.find(item => item.id === patch.asset_id); assert(entry);
            const sheet = entry.template.sheets.find(sheet => sheet.name === entry.template.schema.sheet);
            const allowed = flag => new Set(sheet.rows.flatMap(row => row.cells.filter(cell => cell[flag]).map(cell => sheet.name + '!' + cell.address)));
            for (const key of Object.keys(patch.values)) assert(allowed('editable').has(key), key);
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
          if (session.kind === 'self') for (const entry of session.items) {
            assert(entry.reviewer && entry.reviewed_at && entry.signature.startsWith('data:image/png;base64,') && typeof entry.qualified === 'boolean');
            assert.equal(Object.keys(entry.checks).length, entry.template.schema.checks.length);
          }
          else assert(session.report_name);
          session.status = 'pending'; session.can_edit = false; session.revision++; state.polls = 0; return ok(session);
        }
        if (action === 'download') {
          assert.equal(session.status, 'archived');
          response.writeHead(200, { 'Content-Type': 'application/zip', 'Content-Disposition': 'attachment; filename="fixture-archive.zip"' });
          return response.end(Buffer.from('504b0506000000000000000000000000000000000000', 'hex'));
        }
        if (!action && request.method === 'GET') {
          if (session.status === 'pending') {
            if (state.failPoll) { state.failPoll = false; return fail('归档状态暂时无法读取。', 503); }
            state.polls++;
            session.items.forEach(entry => { entry.archive_status = state.polls < 2 ? 'verifying' : 'archived'; });
            if (state.polls >= 2) session.status = 'archived';
          }
          return ok(session);
        }
      }
      if (request.method !== 'GET') throw new Error('Unexpected write: ' + pathname);
      return ok({ items: [] });
    }
    const name = pathname.startsWith('/assets/') ? pathname.slice(1) : 'index.html';
    const file = path.resolve(dist, name);
    assert(file.startsWith(dist + path.sep));
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
let browser, page;
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
async function fillChecks() { for (const select of await page.locator('.sheet-table select').all()) await select.selectOption('是'); }
async function checkWorksheetLayout() {
  const layout = await page.evaluate(() => {
    const choices = [...document.querySelectorAll('.qualified-options label')].map(label => label.getBoundingClientRect().toJSON());
    const panel = document.querySelector('.editor-panel').getBoundingClientRect().toJSON();
    return { choices, panel, radii: [...document.querySelectorAll('.sheet-table input, .sheet-table select')].map(element => getComputedStyle(element).borderRadius) };
  });
  assert.equal(layout.choices.length, 2);
  assert(Math.abs(layout.choices[0].top - layout.choices[1].top) < 2, 'Qualified choices must stay in one row');
  assert(layout.choices.every(choice => choice.bottom <= layout.panel.bottom), 'Choices stay inside editor');
  assert(layout.radii.every(radius => radius === '2px'), 'Original worksheet inputs retain compact corners');
}
async function noOverflow() {
  const metrics = await page.evaluate(() => ({ viewport: innerWidth, page: document.documentElement.scrollWidth,
    panel: document.querySelector('.tool-workspace').getBoundingClientRect().toJSON() }));
  assert(metrics.page <= metrics.viewport + 1, JSON.stringify(metrics));
  assert(metrics.panel.right <= metrics.viewport + 1, JSON.stringify(metrics));
  return metrics;
}
async function submit() { await button('提交归档').click(); await clickTwice(button('确认提交')); }
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
  await check('left multi-select, template table, retained explicit choice, scope/model/brand/cycle', async () => {
    const unavailable = page.getByRole('checkbox', { name: '选择工具 周期冲突工具', exact: true });
    await unavailable.click();
    assert.equal(await unavailable.getAttribute('aria-checked'), 'false');
    await chooseAssets('电能质量分析仪');
    await chooseRadio('选用模板 电气原表模板');
    await chooseAssets('温湿度仪');
    assert(await radio('选用模板 电气原表模板').isChecked());
    await chooseRadio('使用各工具已关联模板');
    await noOverflow();
    await page.screenshot({ path: path.join(output, 'desktop-selection.png'), fullPage: true });
  });
  await check('repeat open is guarded; partial opening survives reload; resume is bodyless and retryable', async () => {
    state.failOpen = true;
    await clickTwice(button('打开填写'));
    await page.getByText('模板关联中断，请继续打开。', { exact: true }).waitFor();
    assert.equal(count('/open'), 1); assert.equal(state.sessions.size, 1);
    const operation = actions('open')[0].body.operation_id;
    state.failOpen = true;
    await button('打开填写').click();
    await page.getByText('模板关联中断，请继续打开。', { exact: true }).waitFor();
    assert.equal(actions('open')[1].body.operation_id, operation); assert.equal(state.sessions.size, 1);
    await page.reload();
    state.failResume = true;
    await chooseSaved('继续打开');
    await page.getByText('继续打开暂时失败。', { exact: true }).waitFor();
    await chooseSaved('继续打开');
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
    await page.getByLabel('填写 ' + layouts.electrical.schema.sheet + '!C6', { exact: true }).fill('草稿保留内容');
    const select = page.locator('.sheet-table select').first();
    await select.selectOption('是'); await select.selectOption('');
    await button('保存草稿').click();
    await page.getByText('草稿已保存', { exact: true }).waitFor();
    assert.deepEqual(actions('save').at(-1).body.items[0].checks, {});
  });
  await check('failed save keeps all edits; repeat save guarded; switching tools and canceling exit retain data', async () => {
    await page.getByLabel('审核人', { exact: true }).fill('逐台审核员');
    state.failSave = true;
    const before = actions('save').length;
    await clickTwice(button('保存草稿'));
    await page.getByText('模拟保存失败，填写内容保留。', { exact: true }).waitFor();
    assert.equal(actions('save').length, before + 1);
    assert.equal(await page.getByLabel('审核人', { exact: true }).inputValue(), '逐台审核员');
    await button('温湿度仪 · B楼').click();
    assert.equal(await page.locator('.sheet-table tbody tr').count(), layouts.hvac.sheets[1].rows.length);
    await button('电能质量分析仪 · A楼').click();
    assert.equal(await page.getByLabel('审核人', { exact: true }).inputValue(), '逐台审核员');
    await button('返回选择').click();
    await page.getByText('放弃未保存内容？', { exact: true }).waitFor();
    await button('取消').click();
    await button('保存草稿').click();
    await page.getByText('草稿已保存', { exact: true }).waitFor();
  });
  await check('batch review PNG signature, blank signature rejection and individual override visible in native sheet', async () => {
    await button('批量填写审核信息').click();
    await page.getByLabel('批量审核人', { exact: true }).fill('批量审核员');
    await page.getByLabel('批量审核时间', { exact: true }).fill('2026-11-12T09:30');
    assert(await button('覆盖并应用全批').isDisabled());
    await drawSignature('绘制批量签名');
    await button('覆盖并应用全批').click();
    await page.getByLabel('审核人', { exact: true }).fill('电气审核员');
    await page.getByLabel('审核时间', { exact: true }).fill('2026-11-12T10:30');
    await fillChecks();
    await chooseRadio('是');
    await checkWorksheetLayout();
    const review = page.locator('.sheet-table tr').last();
    await review.scrollIntoViewIfNeeded();
    assert.match(await review.innerText(), /电气审核员/); assert.match(await review.innerText(), /2026-11-12 10:30/);
    assert(await review.getByAltText('表内审核签名').isVisible());
    await page.screenshot({ path: path.join(output, 'desktop-electrical-review.png'), fullPage: true });
    await button('温湿度仪 · B楼').click();
    assert.equal(await page.locator('.sheet-scroll').evaluate(element => element.scrollTop), 0, 'Changing tools resets only worksheet scroll');
    assert.equal(await page.getByLabel('审核人', { exact: true }).inputValue(), '批量审核员');
    await fillChecks(); await chooseRadio('是');
    await page.getByLabel('填写 ' + layouts.hvac.schema.sheet + '!E20', { exact: true }).fill('23.5');
    await page.locator('.sheet-table tr').last().scrollIntoViewIfNeeded();
    assert.match(await page.locator('.sheet-table tr').last().innerText(), /批量审核员/);
    await page.screenshot({ path: path.join(output, 'desktop-hvac-review.png'), fullPage: true });
  });
  await check('submit stops on save failure and retains draft; polling recovers and downloads archived ZIP', async () => {
    state.failSave = true;
    await submit();
    await page.getByText('模拟保存失败，填写内容保留。', { exact: true }).waitFor();
    assert.equal(actions('submit').length, 0);
    assert.equal(await page.getByLabel('填写 ' + layouts.hvac.schema.sheet + '!E20', { exact: true }).inputValue(), '23.5');
    state.failPoll = true;
    await submit();
    await page.getByText('归档处理中', { exact: true }).waitFor();
    assert.equal(actions('submit').length, 1);
    assert(await page.getByLabel('审核人', { exact: true }).isDisabled());
    await page.getByText('归档状态暂时无法读取。', { exact: true }).waitFor();
    await page.getByRole('link', { name: '下载归档', exact: true }).waitFor({ timeout: 20000 });
    const download = page.waitForEvent('download');
    await page.getByRole('link', { name: '下载归档', exact: true }).click();
    assert.equal((await download).suggestedFilename(), 'fixture-archive.zip');
  });
  await check('external tools share one binary report; upload failure retains file, repeat upload/submit guarded', async () => {
    await button('返回选择').click();
    await chooseRadio('送检');
    await chooseAssets('送检压力表', '送检绝缘表');
    await button('打开送检').click();
    await page.getByText('2 台工具共用一份送检报告。', { exact: true }).waitFor();
    await work().locator('input[type=file]').setInputFiles({ name: '共用报告.pdf', mimeType: 'application/pdf', buffer: Buffer.from('%PDF-1.4\nfixture only') });
    state.failReport = true;
    await clickTwice(button('上传报告'));
    await page.getByText('模拟报告上传失败。', { exact: true }).waitFor();
    await button('上传报告').waitFor(); assert.equal(actions('report').length, 1);
    await button('上传报告').click();
    await page.getByText('已保存报告 · 共用报告.pdf', { exact: true }).waitFor();
    assert.equal(actions('report').length, 2);
    await submit();
    await page.getByRole('link', { name: '下载归档', exact: true }).waitFor();
    assert.equal([...state.sessions.values()].at(-1).items.length, 2);
  });
  await check('explicit single template replacement requires confirmation and applies to both selected tools', async () => {
    reset(); await page.goto(legacy);
    await chooseAssets('电能质量分析仪', '温湿度仪');
    await chooseRadio('选用模板 暖通原表模板');
    await button('打开填写').click();
    await page.getByText('更换所选工具的模板', { exact: true }).waitFor();
    await button('取消').click(); assert.equal(count('/open'), 0);
    await button('打开填写').click(); await clickTwice(button('确认'));
    await page.getByRole('heading', { name: '自检记录', exact: true }).waitFor();
    assert.equal(count('/open'), 1);
    assert.equal(actions('open')[0].body.template_id, 'hvac');
    assert([...state.sessions.values()][0].items.every(entry => entry.template.schema.layout === 'hvac'));
  });
  await check('V2 desktop and mobile selection/editor; multiple templates remain separate', async () => {
    reset(); await page.goto(v2);
    await page.getByRole('heading', { name: '工具台账' }).waitFor();
    await noOverflow();
    await page.screenshot({ path: path.join(output, 'v2-desktop-selection.png'), fullPage: true });
    await page.setViewportSize({ width: 390, height: 844 });
    await noOverflow();
    await page.screenshot({ path: path.join(output, 'mobile-selection.png'), fullPage: true });
    await chooseAssets('电能质量分析仪', '温湿度仪');
    await button('打开填写').click();
    await page.getByRole('heading', { name: '自检记录', exact: true }).waitFor();
    await page.getByLabel('审核人', { exact: true }).fill('手机审核员');
    await page.getByLabel('审核时间', { exact: true }).fill('2026-11-12T11:30');
    await drawSignature('手绘签名');
    await checkWorksheetLayout();
    await noOverflow();
    await page.screenshot({ path: path.join(output, 'mobile-editor.png'), fullPage: true });
    await button('温湿度仪 · B楼').click();
    assert.equal(await page.getByLabel('审核人', { exact: true }).inputValue(), '');
    await button('电能质量分析仪 · A楼').click();
    assert.equal(await page.getByLabel('审核人', { exact: true }).inputValue(), '手机审核员');
    await button('保存草稿').click(); await page.getByText('草稿已保存', { exact: true }).waitFor();
  });
  await check('200 tool tabs stay in one scrollable row; batch review stays outside on mobile and desktop', async () => {
    reset();
    state.assets = Array.from({ length: 200 }, (_, index) => ({ ...clone(initialAssets[0]), id: 'bulk-' + index, name: '批量工具' + (index + 1) }));
    await page.goto(v2);
    await page.getByRole('checkbox', { name: '选择筛选后的工具', exact: true }).click();
    await button('打开填写').click();
    await page.getByRole('heading', { name: '自检记录', exact: true }).waitFor({ timeout: 30000 });
    assert.equal(await page.locator('.tool-tabs button').count(), 200);
    for (const width of [390, 1440]) {
      await page.setViewportSize({ width, height: 1000 });
      await noOverflow();
      const layout = await page.evaluate(() => {
        const tabs = document.querySelector('.tool-tabs');
        const buttons = [...tabs.querySelectorAll('button')];
        const batch = document.querySelector('.tool-toolbar > button');
        return { tops: buttons.map(button => button.offsetTop), height: tabs.offsetHeight,
          clientWidth: tabs.clientWidth, scrollWidth: tabs.scrollWidth, batchLeft: batch.getBoundingClientRect().left,
          tabsRight: tabs.getBoundingClientRect().right };
      });
      assert.equal(new Set(layout.tops).size, 1);
      assert(layout.height < 75 && layout.scrollWidth > layout.clientWidth);
      assert(layout.batchLeft >= layout.tabsRight, 'Batch review stays outside the scroll area');
      await page.screenshot({ path: path.join(output, `tabs-200-${width}.png`), fullPage: true });
    }
    await button('批量工具200 · A楼').click();
    assert.equal(await button('批量工具200 · A楼').getAttribute('aria-pressed'), 'true');
    assert.equal(await page.locator('.sheet-scroll').evaluate(element => element.scrollTop), 0);
  });
  assert.deepEqual(errors, []); assert.deepEqual(serverErrors, []); assert.deepEqual(blocked, []);
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
  if (process.env.TOOL_WORKSPACE_KEEP_PREVIEW === '1' && results.length === 11) {
    reset();
    await writeFile(path.join(output, 'preview-server.json'), JSON.stringify({ pid: process.pid, legacy, v2, fixturesOnly: true }, null, 2));
    console.log('Fixture preview remains available: ' + v2);
  } else await new Promise(resolve => server.close(resolve));
}
