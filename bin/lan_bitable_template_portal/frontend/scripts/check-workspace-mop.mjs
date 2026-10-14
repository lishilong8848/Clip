import assert from 'node:assert/strict';
import { mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/workspace-mop');
const before = process.argv.includes('--before');
const site = process.env.CLIPFLOW_MOP_PREVIEW_SITE || path.resolve(root, before ? '../../../output/header-polish-build' : 'dist');
await mkdir(output, { recursive: true });
const server = await preview({ root, configFile: false, logLevel: 'error', build: { outDir: site }, preview: { host: '127.0.0.1', port: 0 } });
const origin = 'http://127.0.0.1:' + server.httpServer.address().port;
const browser = await chromium.launch({ headless: true });
const errors = [], writes = [], reads = [];
const titles = ['冷水机组月度维护', 'BA系统资料维护', '空调过滤网更换维护', '旁滤装置月度维护', '制冷单元季度维护', '水处理系统维护'];
const notices = Array.from({ length: 12 }, (_, i) => ({ notice_key: 'fixture-notice-' + i, source_record_id: 'fixture-source-' + i,
  title: `EA118机房A楼${titles[i % titles.length]}${i > 5 ? '（第二阶段）' : ''}`, building: 'A楼', scope: 'A', specialty: '暖通',
  status: i > 8 ? '已结束' : '开始', maintenance_cycle: '月度', start_time: '2026-10-14 09:00', end_time: '2026-10-14 18:30',
  mop_uploaded: i > 8, mop_attachment_count: i > 8 ? 1 : 0 }));
const candidates = Array.from({ length: 15 }, (_, i) => ({ record_id: 'fixture-mop-' + i, title: titles[i % titles.length] + '标准作业指导书',
  file_no: 'JSNT-MOP-' + String(101 + i), specialty: '暖通', maintenance_type: '月度', version: 'V1.2', file_status: '有效', attachment_count: 2,
  fields: { 文件编号: 'JSNT-MOP-' + String(101 + i), 专业: '暖通', 维护类型: '月度' },
  attachments: [{ file_token: 'fixture-file-' + i, name: '维护记录表.xlsx' }, { file_token: 'fixture-cover-' + i, name: '维护计划表.xlsx' }] }));
const rows = [['设备维护记录表', '', '', '', '', '', ''], ['序号', '设备编号', '检查项目', '检查要求', '正常/异常', '检查结果', '备注'],
  ...Array.from({ length: 28 }, (_, i) => [String(i + 1), 'A-CH-' + String(i + 1).padStart(3, '0'), ['运行状态', '水系统', '电气检查'][i % 3], '按作业指导书核查', '', '', ''])];
const sheet = { name: '维护记录', is_cover: false, row_count: rows.length, column_count: 7, rows, merged_cells: [],
  checkbox_cells: [{ row: 11, col: 4, cell_ref: 'E12' }, { row: 12, col: 4, cell_ref: 'E13' }], maintenance_fields: [] };
let page;
try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
  await context.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url());
    if (url.origin !== origin) return route.abort();
    if (url.pathname === '/assets/vnet-logo.png') return route.fulfill({ contentType: 'image/png', body: await readFile(path.join(root, 'public/assets/vnet-logo.png')) });
    if (url.pathname === '/assets/connection-guard.js') return route.fulfill({ contentType: 'application/javascript', body: '' });
    if (!url.pathname.startsWith('/api/')) return route.continue();
    const ok = data => route.fulfill({ json: { ok: true, data } });
    reads.push(url.pathname + url.search);
    if (url.pathname === '/api/auth/status') return ok({ logged_in: true, user: { open_id: 'mop-layout-fixture', name: '界面预览', role: 'admin', level: 'L1', category: 'manager', modules: ['engineer_mop'] }, scope_options: [{ value: 'A', label: 'A楼' }, { value: 'B', label: 'B楼' }] });
    if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-mop' } });
    if (url.pathname === '/api/engineer/mop/bootstrap') return ok({ notices: url.searchParams.get('scope') === 'B' ? [] : structuredClone(notices), mop_candidates: candidates, warnings: [], auth: { user: { open_id: 'mop-layout-fixture', name: '界面预览' } } });
    if (url.pathname === '/api/engineer/mop/bind') {
      const body = req.postDataJSON(); writes.push(body);
      assert(body.notice_key.startsWith('fixture-notice-'));
      return ok({ binding: body });
    }
    if (url.pathname === '/api/engineer/mop/preview') return ok({ local_file: { path: 'fixture.xlsx', file_name: '维护记录表.xlsx' }, mop_title: '冷水机组维护记录', attachment: { name: '维护记录表.xlsx' }, sheets: [sheet, { name: '封面', is_cover: true, row_count: 1, column_count: 2, rows: [['设备维护作业', 'A楼']] }], warnings: [] });
    assert.equal(req.method(), 'GET', 'Unexpected business action in isolated preview: ' + url.pathname);
    return ok({ people: [], items: [], total: 0, signatures: [], records: [], summary: {} });
  });
  page = await context.newPage(); page.setDefaultTimeout(15000);
  page.on('pageerror', error => errors.push(error.message));
  await page.goto(origin + '/assets/workspace-v2/index.html?__page=/engineer/mop&scope=A');
  await page.getByRole('button', { name: '打开填写', exact: true }).waitFor();
  await page.screenshot({ path: path.join(output, before ? 'before-desktop.png' : 'desktop.png'), fullPage: before });
  if (!before) {
    assert.equal(await page.locator('.v2-mop-workspace').count(), 1);
    assert.equal(await page.locator('[aria-label="MOP填写流程"]:visible').count(), 1, 'One progress strip, not two');
    const left = await page.locator('.notice-panel').boundingBox(), right = await page.locator('.binding-panel').boundingBox();
    assert(left.x + left.width <= right.x, 'Desktop keeps notice and MOP selection side by side');
    const upload = await page.locator('.local-mop-upload').boundingBox();
    assert(upload.y + upload.height < 900, 'Local-file upload stays in the desktop viewport');
    for (const [width,height] of [[1366,768],[1024,768],[390,844]]) {
      await page.setViewportSize({width,height});
      await page.screenshot({path:path.join(output,`binding-${width}.png`),animations:'disabled'});
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1),false,'binding page stays within viewport');
      const panel=await page.locator('.binding-panel').boundingBox(),card=await page.locator('.mop-selected-file-card').boundingBox();
      assert(card.x>=panel.x&&card.x+card.width<=panel.x+panel.width+1,'selected file stays inside its panel');
      if(width>=1366){const box=await page.locator('.local-mop-upload').boundingBox();assert(box.y+box.height<=height,'local upload must remain reachable on a 768px-high desktop');}
    }
    await page.setViewportSize({width:1440,height:900});
    const search = page.getByPlaceholder('搜索通告、楼栋、专业');
    await search.fill('BA系统'); assert.equal(await page.locator('.notice-row').count(), 2);
    await page.locator('.notice-row').first().click();
    await page.locator('.selected-notice').getByText('EA118机房A楼BA系统资料维护', { exact: true }).waitFor();
    await search.fill('');
    await page.getByPlaceholder('搜索 MOP 名称、文件编号、专业').fill('JSNT-MOP-102');
    assert.equal(await page.locator('.mop-row').count(), 1);
    await page.locator('.mop-row').click();
    await page.locator('.attachment-select select').selectOption('fixture-cover-1');
    await page.getByRole('button', { name: '打开填写', exact: true }).click();
    await page.locator('.preview-scroll table').waitFor();
    assert.equal(writes.length, 1);
    assert.equal(writes[0].notice_key, 'fixture-notice-1');
    assert.equal(writes[0].mop_record_id, 'fixture-mop-1');
    assert.equal(writes[0].mop_attachment_token, 'fixture-cover-1');
    const footer = await page.locator('.floating-upload-action-row').boundingBox();
    assert(footer.y + footer.height < 900, 'Editor actions stay in the desktop viewport');
    await page.locator('.signature-role-summary button').first().click();
    const dialog = page.getByRole('dialog', { name: '维护人员签名', exact: true });
    await dialog.waitFor();
    await dialog.getByRole('button', { name: '关闭', exact: true }).click();
    await dialog.waitFor({ state: 'hidden' });
    await page.screenshot({ path: path.join(output, 'editor-desktop.png') });
    await page.getByRole('button', { name: '第10行后全正常', exact: true }).click();
    await page.locator('.preview-scroll td.normal').first().waitFor();
    await page.getByRole('button', { name: '封面', exact: true }).click();
    await page.locator('.preview-scroll').getByText('设备维护作业', { exact: true }).waitFor();
    await page.getByRole('button', { name: '维护记录', exact: true }).click();
    for (const width of [1366, 1920, 390]) {
      await page.setViewportSize({ width, height: width < 700 ? 844 : 900 });
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false, 'No page overflow');
      if (width === 390) await page.screenshot({ path: path.join(output, 'editor-mobile.png') });
    }
    await page.locator('.vnet-back-button:visible').click();
    await page.getByRole('button', { name: '打开填写', exact: true }).waitFor();
    await page.getByPlaceholder('搜索通告、楼栋、专业').fill('冷水');
    await page.locator('.notice-row').first().click();
    await page.locator('.selected-notice').getByText('EA118机房A楼冷水机组月度维护', { exact: true }).waitFor();
    await page.screenshot({ path: path.join(output, 'mobile.png') });
    assert.equal(writes.length, 1, 'Choosing notices does not submit a business action');
    await page.getByRole('combobox', { name: '切换楼栋', exact: true }).selectOption('B');
    await page.getByText('暂无维保通告', { exact: true }).waitFor();
    assert(reads.some(url => url.includes('bootstrap?scope=B')));
  }
  assert.deepEqual(errors, []);
  console.log(before ? 'Captured existing MOP layout.' : 'PASS: native MOP controller, filters, selection, attachment, bind/preview payload, sheet editing, return, empty scope and PC/mobile layout. All APIs mocked.');
} catch (error) {
  if (page) { await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true }); console.error(await page.locator('body').innerText()); }
  throw error;
} finally { await browser.close(); await server.close(); }
