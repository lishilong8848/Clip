import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';
import { buildMopPreview } from './build-mop-mobile-preview.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const site = await buildMopPreview();
const output = path.resolve(root, '../../../output/playwright/mop-mobile-editing');
await mkdir(output, { recursive: true });
const server = await preview({ root, configFile: false, logLevel: 'error', build: { outDir: site }, preview: { host: '127.0.0.1', port: 0 } });
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
const browser = await chromium.launch({ headless: true });
const errors = [];
const rows = [
  ['冷水机组维护记录', '', '', '', '', '', ''],
  ['维护开始时间', '', '', '', '', '', ''],
  ['维护完成情况', '', '', '', '', '', ''],
  ['维护涉及位置：', '', '', '', '', '', ''],
  ['维护完成时间', '', '', '', '', '', ''],
  ['审核确认时间', '', '', '', '', '', ''],
  ...Array.from({ length: 3 }, () => ['', '', '', '', '', '', '']),
  ['序号', '设备编号', '检查项目', '检查要求', '检查结果', '备注', '复核备注'],
  ...Array.from({ length: 90 }, (_, i) => [String(i + 1), `A-CH-${i + 1}`, '水系统', '检查运行状态、接口及压力是否正常', '', '', '']),
];
const sheet = { name: '维护记录', row_count: rows.length, column_count: 7, rows, merges: [{ row: 0, col: 0, rowspan: 1, colspan: 7 }],
  checkbox_cells: [{ row: 10, col: 4, cell_ref: 'E11' }, { row: 11, col: 4, cell_ref: 'E12' }], maintenance_fields: [
    { row: 1, label_col: 0, value_col: 1, label: '维护开始时间', value_cell_ref: 'B2' },
    { row: 2, label_col: 0, value_col: 1, label: '维护完成情况', value_cell_ref: 'B3' },
    { row: 4, label_col: 0, value_col: 1, label: '维护完成时间', value_cell_ref: 'B5' },
    { row: 5, label_col: 0, value_col: 1, label: '审核确认时间', value_cell_ref: 'B6' },
  ] };
const candidate = { record_id: 'fixture-mop', title: '冷水机组维护记录', specialty: '暖通', attachments: [{ file_token: 'fixture-file', name: '维护记录.xlsx' }] };
const notice = { notice_key: 'fixture-notice', source_record_id: 'fixture-source', title: 'A楼冷水机组月度维护', building: 'A楼', scope: 'A', status: '开始', specialty: '暖通' };

try {
  for (const version of ['v2', 'legacy']) {
    const context = await browser.newContext({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true, reducedMotion: 'reduce' });
    const writes = [], requests = [];
    await context.route('**/*', async route => {
      const req = route.request(), url = new URL(req.url());
      if (url.origin !== origin) return route.abort();
      if (url.pathname === '/assets/connection-guard.js') return route.fulfill({ contentType: 'application/javascript', body: '' });
      if (!url.pathname.startsWith('/api/')) return route.continue();
      requests.push(`${req.method()} ${url.pathname}`);
      const ok = data => route.fulfill({ json: { ok: true, data } });
      if (url.pathname === '/api/auth/status') return ok({ logged_in: true, user: { open_id: 'fixture-mop-user', name: '预览人员', role: 'admin', level: 'L1', category: 'manager', modules: ['engineer_mop'] }, scope_options: [{ value: 'A', label: 'A楼' }] });
      if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'fixture-mop-mobile' } });
      if (url.pathname === '/api/engineer/mop/bootstrap') return ok({ notices: [notice], mop_candidates: [candidate], warnings: [], auth: { user: { open_id: 'fixture-mop-user' } } });
      if (url.pathname === '/api/engineer/mop/preview') return ok({ local_file: { path: 'fixture.xlsx', file_name: '维护记录.xlsx' }, mop_title: '冷水机组维护记录', attachment: candidate.attachments[0], sheets: [{ name: '封面', is_cover: true, row_count: 1, column_count: 1, rows: [['维护记录封面']] }, sheet], warnings: [] });
      if (url.pathname === '/api/engineer/mop/bind') { writes.push(req.postDataJSON()); return ok({ binding: writes.at(-1) }); }
      assert.equal(req.method(), 'GET', `Unexpected write: ${url.pathname}`);
      return ok({ items: [], people: [], records: [], signatures: [], total: 0, summary: {} });
    });
    const page = await context.newPage();
    page.setDefaultTimeout(10000);
    page.on('pageerror', error => errors.push(error.message));
    page.on('console', message => { if (message.type() === 'error') errors.push(message.text()); });
    await page.goto(`${origin}/?version=${version}&scope=A`);
    await page.getByRole('button', { name: '打开填写', exact: true }).click();
    const form = page.getByRole('region', { name: '维护单快捷填写' });
    await form.waitFor().catch(async error => {
      await page.screenshot({ path: path.join(output, `${version}-open-failure.png`), fullPage: true });
      console.error(errors, requests, await page.locator('body').innerText());
      throw error;
    });
    assert.equal(await page.locator('.preview-scroll table').count(), 0, 'Phone opens quick form instead of full spreadsheet');
    assert.equal(writes.length, 1);
    const cell = (row, col) => page.locator(`[data-mop-cell-key="维护记录:${row}:${col}"]`);
    const editor = page.getByRole('group', { name: '填写单元格', exact: true });
    async function tapCell(row, col) {
      await cell(row, col).tap();
      await editor.waitFor();
    }
    async function checkPanel() {
      const box = await editor.boundingBox();
      const viewport = page.viewportSize();
      assert(box.x >= 0 && box.x + box.width <= viewport.width + 1, `Editor fits phone width: ${version} ${JSON.stringify(box)} viewport ${viewport.width}`);
      assert(box.y >= 0 && box.y + box.height <= viewport.height + 1, 'Editor fits visible height');
      for (const button of await editor.locator('button:visible').all()) {
        const bounds = await button.boundingBox();
        assert(bounds.height >= 44 && bounds.width >= 44, 'Touch controls are at least 44px');
      }
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false, 'No horizontal page overflow');
    }
    try {
      await form.getByLabel('维护涉及位置', { exact: true }).fill('A楼三层冷站');
      await form.getByLabel('维护开始时间', { exact: true }).fill('2026-10-15T09:00');
      assert.equal(await form.getByRole('button', { name: '第10行后全正常', exact: true }).count(), 0, 'Details page excludes check results');
      assert.equal(await page.locator('.floating-upload-box').isVisible(), false, 'Upload stays on the final page');
      await page.getByRole('group', { name: '签名角色' }).getByRole('button').first().tap();
      const signatureDialog = page.getByRole('dialog', { name: '维护人员签名', exact: true });
      await signatureDialog.waitFor();
      const signatureBounds = await signatureDialog.boundingBox();
      assert(signatureBounds.x >= 0 && signatureBounds.x + signatureBounds.width <= 391, 'Signature panel fits phone');
      await signatureDialog.getByRole('button', { name: '关闭', exact: true }).tap();
      await signatureDialog.waitFor({ state: 'hidden' });
      for (const width of [360, 390, 430]) {
        await page.setViewportSize({ width, height: 844 });
        assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
        await page.screenshot({ path: path.join(output, `${version}-${width}-people-time.png`), fullPage: true });
      }
      await page.getByRole('button', { name: '继续：检查与备注', exact: true }).tap();
      assert.equal(await page.getByRole('group', { name: '签名角色' }).isVisible(), false, 'Check page excludes signature section');
      assert.equal(await form.getByLabel('维护涉及位置', { exact: true }).count(), 0);
      assert.equal(await form.getByLabel('维护开始时间', { exact: true }).count(), 0);
      await form.getByRole('button', { name: '第10行后全正常', exact: true }).tap();
      await form.getByRole('button', { name: '异常', exact: true }).tap();
      await form.getByRole('textbox', { name: 'A-CH-1 · 水系统 备注', exact: true }).fill('接口轻微渗水，已记录并安排复查。');
      await form.getByLabel('维护完成情况', { exact: true }).selectOption('已完成[√] 未完成[ ]');
      await form.getByRole('button', { name: '下一项', exact: true }).tap();
      await form.getByRole('textbox', { name: 'A-CH-2 · 水系统 备注', exact: true }).fill('另一台设备的备注');
      await form.getByRole('button', { name: '上一项', exact: true }).tap();
      await page.getByRole('button', { name: '人员与时间', exact: true }).tap();
      assert.equal(await form.getByLabel('维护涉及位置', { exact: true }).inputValue(), 'A楼三层冷站');
      assert.equal(await form.getByLabel('维护开始时间', { exact: true }).inputValue(), '2026-10-15T09:00');
      await page.getByRole('button', { name: '检查与备注', exact: true }).tap();
      assert.equal(await form.getByRole('textbox', { name: 'A-CH-1 · 水系统 备注', exact: true }).inputValue(), '接口轻微渗水，已记录并安排复查。');
      for (const width of [360, 390, 430]) {
        await page.setViewportSize({ width, height: 844 });
        assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false, 'Quick form fits phone');
        await form.scrollIntoViewIfNeeded();
        await page.screenshot({ path: path.join(output, `${version}-${width}-quick-form.png`), fullPage: true });
      }
      await page.getByRole('button', { name: '查看原表', exact: true }).tap();
      assert.equal(await cell(3, 1).textContent(), 'A楼三层冷站');
      assert.match(await cell(10, 4).textContent(), /☑异常/);
      assert.match(await cell(11, 4).textContent(), /☑正常/);
      assert.match(await cell(11, 5).textContent(), /另一台设备/);
      for (const width of [360, 390, 430]) {
        await page.setViewportSize({ width, height: 844 });
        await tapCell(10, 4);
        await editor.getByRole('button', { name: '异常', exact: true }).tap();
        assert.match(await cell(10, 4).textContent(), /☑异常/);
        await checkPanel();
        await page.screenshot({ path: path.join(output, `${version}-${width}-choice.png`) });
        await editor.getByRole('button', { name: '下一格', exact: true }).tap();
        const input = editor.getByRole('textbox', { name: 'F11 普通单元格', exact: true });
        await input.fill('接口轻微渗水，已记录并安排复查。');
        await checkPanel();
        await page.screenshot({ path: path.join(output, `${version}-${width}-text.png`) });
        await editor.getByRole('button', { name: '下一格', exact: true }).tap();
        await editor.getByRole('textbox', { name: 'G11 普通单元格', exact: true }).fill('待复核');
        await editor.getByRole('button', { name: '上一格', exact: true }).tap();
        assert.equal(await input.inputValue(), '接口轻微渗水，已记录并安排复查。');
        await editor.getByRole('button', { name: '完成填写', exact: true }).tap();
        await editor.waitFor({ state: 'hidden' });
      }
      await tapCell(1, 1);
      await editor.getByLabel('维护开始时间', { exact: true }).fill('2026-10-15T09:00');
      await editor.getByRole('button', { name: '填入', exact: true }).tap();
      assert.match(await cell(1, 1).textContent(), /2026.*10.*15.*09时00/);
      await editor.getByRole('button', { name: '完成填写', exact: true }).tap();
      await tapCell(2, 1);
      await editor.getByRole('button', { name: '已完成', exact: true }).tap();
      assert.match(await cell(2, 1).textContent(), /已完成\[√\]/);
      await editor.getByRole('button', { name: '完成填写', exact: true }).tap();
      await tapCell(10, 5);
      // Emulate a keyboard-reduced visual viewport without writing business data.
      await page.evaluate(() => {
        Object.defineProperty(visualViewport, 'height', { configurable: true, value: 380 });
        visualViewport.dispatchEvent(new Event('resize'));
      });
      await page.waitForFunction(() => document.querySelector('.mobile-cell-editor').getBoundingClientRect().bottom <= 381);
      assert((await editor.boundingBox()).y >= 0);
      await page.screenshot({ path: path.join(output, `${version}-keyboard.png`) });
      await page.evaluate(() => { delete visualViewport.height; visualViewport.dispatchEvent(new Event('resize')); });
      await editor.getByRole('button', { name: '完成填写', exact: true }).tap();
      await page.getByRole('button', { name: '封面', exact: true }).tap();
      await page.locator('[data-mop-cell-key="封面:0:0"]').tap();
      assert.equal(await editor.count(), 0, 'Cover remains read-only');
      await page.getByRole('button', { name: '维护记录', exact: true }).tap();
      assert.match(await cell(10, 5).textContent(), /接口轻微渗水/);
      await page.setViewportSize({ width: 1440, height: 900 });
      await cell(10, 4).click();
      await page.locator('.floating-cell-popover:not(.mobile-cell-editor)').waitFor();
      assert.equal(await editor.count(), 0, 'Desktop keeps its inline editor');
      await page.screenshot({ path: path.join(output, `${version}-desktop.png`) });
      assert.equal(writes.length, 1, 'Editing never submits or uploads automatically');
    } catch (error) {
      await page.screenshot({ path: path.join(output, `${version}-failure.png`), fullPage: true });
      throw error;
    } finally { await context.close(); }
  }
  assert.deepEqual(errors, []);
  console.log('PASS: legacy/V2 mobile 360/390/430, touch edits, next/previous, text preservation, date/completion, keyboard viewport, read-only cover, desktop regression. APIs isolated.');
} finally { await browser.close(); await server.close(); }
