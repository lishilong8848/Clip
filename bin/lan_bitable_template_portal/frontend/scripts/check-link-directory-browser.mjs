import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/link-directory');
await mkdir(output, { recursive: true });
const fixture = Array.from({ length: 329 }, (_, i) => ({ name: i === 0 ? '机柜基础资料' : `示例表 ${i}`,
  url: `https://vnet.feishu.cn/base/example?table=tblExample${i}`, purpose: '隔离测试用途', sort: i + 1,
  category: i === 0 ? '设备管理' : '南通全景驾驶舱-维护管理' }));
fixture.push({ name: '平台门户', url: 'https://www.sm.sjhl.online:3001/nantong-base', purpose: '网站入口', sort: 330, category: '网页导航' });
fixture.push({ name: '培训持证', url: 'https://www.sm.sjhl.online:3001/training-certification-management?module=certificate', purpose: '网站入口', sort: 331, category: '网页导航' });
fixture.push({ name: '孪生图谱', url: 'https://www.sm.sjhl.online:8787/', purpose: '网站入口', sort: 332, category: '网页导航' });
let rows = fixture.map((row, i) => ({ ...row, id: `rec${i + 1}` }));
let admin = true, failSave = false, failRefresh = false, warning = '', writes = [], reads = 0;
const server = await preview({ root, logLevel: 'error', preview: { host: '127.0.0.1', port: 0, strictPort: false } });
const base = `http://127.0.0.1:${server.httpServer.address().port}`;
const browser = await chromium.launch({ headless: true });
const watchdog = setTimeout(() => { console.error('Directory browser test exceeded 90 seconds'); void browser.close(); }, 90000);
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
page.setDefaultTimeout(15000);
const errors = [];
page.on('pageerror', e => errors.push(e.message));
await page.addInitScript(() => Object.defineProperty(crypto, 'randomUUID', { value: undefined, configurable: true }));
await page.route('**/api/**', async route => {
  const req = route.request(), url = new URL(req.url()), method = req.method();
  const ok = data => route.fulfill({ json: { ok: true, data } });
  const fail = error => route.fulfill({ status: 502, json: { ok: false, error } });
  if (url.pathname === '/api/auth/status') return ok({ logged_in: true, user: { open_id: 'link-isolated', role: admin ? 'admin' : 'building', name: '隔离测试' }, scope_options: [{ value: 'A', label: 'A楼' }] });
  if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-directory' } });
  if (url.pathname.startsWith('/api/link-directory')) {
    if (method === 'GET' || url.pathname.endsWith('/refresh')) {
      reads++;
      if (failRefresh) { failRefresh = false; return fail('云端暂时不可用'); }
      return ok({ items: rows, can_edit: admin, updated_at: Date.now() / 1000, stale: !!warning, error: warning });
    }
    const body = req.postDataJSON() || {};
    writes.push({ path: url.pathname, method, body });
    if (failSave) { failSave = false; return fail('保存失败，请重试'); }
    if (url.pathname.endsWith('/reorder')) {
      assert.deepEqual(Object.keys(body).sort(), ['placement', 'record_id', 'target_id']);
      rows.sort((a, b) => a.sort - b.sort || a.name.localeCompare(b.name) || a.id.localeCompare(b.id));
      const source = rows.find(row => row.id === body.record_id);
      assert.ok(source);
      rows = rows.filter(row => row.id !== body.record_id);
      const target = rows.findIndex(row => row.id === body.target_id);
      assert.ok(target >= 0);
      rows.splice(target + (body.placement === 'after' ? 1 : 0), 0, source);
      rows.forEach((row, index) => { row.sort = (index + 1) * 10; });
      return ok({ items: rows, can_edit: admin, updated_at: Date.now() / 1000 });
    }
    if (method === 'POST') {
      assert.match(body.request_id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
      const row = { ...body, id: 'recCreated' }; delete row.request_id; rows.push(row); return ok({ item: row });
    }
    const id = url.pathname.split('/').pop();
    if (method === 'PUT') { rows = rows.map(row => row.id === id ? { ...body, id } : row); return ok({ item: rows.find(row => row.id === id) }); }
    rows = rows.filter(row => row.id !== id); return ok({ deleted: true });
  }
  if (url.pathname === '/api/assistant/conversation') return ok({ enabled: false, turns: [] });
  return ok({});
});
const search = page.getByRole('searchbox', { name: '按名称、用途或分类搜索' });
const tableRows = page.locator('.link-table tbody tr');
try {
  await page.goto(base + '/');
  const entry = page.getByRole('button', { name: '多维表导航', exact: true });
  await entry.waitFor();
  const box = await entry.boundingBox();
  assert.ok(box.x < 100 && box.y > 900, 'home entry is bottom left');
  await page.screenshot({ path: path.join(output, 'home-entry.png'), fullPage: true });
  await entry.click();
  await page.getByRole('heading', { name: '多维表导航', exact: true }).waitFor();
  await tableRows.first().waitFor();
  await page.waitForFunction(() => !document.querySelector('.ui-page-leave-active, .ui-page-enter-active'));
  assert.equal(await tableRows.count(), 25);
  await page.getByRole('button', { name: '网页导航', exact: true }).click();
  assert.equal(await tableRows.count(), 3);
  assert.equal(await page.getByRole('link', { name: '培训持证', exact: true }).getAttribute('href'), fixture[330].url);
  assert.equal(await page.getByRole('link', { name: '孪生图谱', exact: true }).getAttribute('href'), fixture[331].url);
  await page.screenshot({ path: path.join(output, 'websites.png'), fullPage: true });
  await page.getByRole('button', { name: '全部', exact: true }).click();
  assert.equal(await tableRows.locator('a:not([rel="noopener noreferrer"])').count(), 0);
  await page.getByRole('button', { name: '下一页', exact: true }).click();
  assert.match(await page.locator('.table-footer b').innerText(), /2/);
  await search.fill('机柜基础资料');
  assert.equal(await tableRows.count(), 1);
  await page.getByRole('combobox', { name: '按分类筛选' }).selectOption('南通全景驾驶舱-维护管理');
  await page.getByText('没有匹配的链接', { exact: true }).waitFor();
  await search.fill('');
  await page.getByRole('combobox', { name: '按分类筛选' }).selectOption('');
  await page.screenshot({ path: path.join(output, 'directory.png'), fullPage: true });
  await page.getByRole('button', { name: '新增链接', exact: true }).click();
  const modal = page.getByRole('dialog', { name: '新增链接', exact: true });
  await modal.getByLabel('名称', { exact: false }).fill('隔离测试链接');
  await modal.getByLabel('链接 URL', { exact: false }).fill('javascript:alert(1)');
  await modal.getByRole('button', { name: '保存', exact: true }).click();
  assert.equal(writes.length, 0, 'unsafe URL cannot be submitted');
  await modal.getByLabel('链接 URL', { exact: false }).fill('https://www.sm.sjhl.online:3001/example?module=unit-test#section');
  await modal.getByLabel('用途', { exact: true }).fill('仅用于隔离测试');
  await page.screenshot({ path: path.join(output, 'editor.png'), fullPage: true });
  failSave = true;
  await modal.getByRole('button', { name: '保存', exact: true }).click();
  await modal.getByText('保存失败，请重试', { exact: true }).waitFor();
  assert.equal(await modal.getByLabel('名称', { exact: false }).inputValue(), '隔离测试链接');
  const retryToken = writes[0].body.request_id;
  await modal.getByRole('button', { name: '保存', exact: true }).click();
  await modal.waitFor({ state: 'hidden' });
  assert.equal(writes[1].body.request_id, retryToken);
  await search.fill('隔离测试链接');
  await tableRows.first().waitFor();
  await tableRows.getByRole('button', { name: '编辑链接', exact: true }).click();
  const edit = page.getByRole('dialog', { name: '编辑链接', exact: true });
  await edit.getByLabel('名称', { exact: false }).fill('编辑后的链接');
  await edit.getByRole('button', { name: '保存', exact: true }).click();
  await edit.waitFor({ state: 'hidden' });
  await search.fill('编辑后的链接');
  assert.equal(await tableRows.count(), 1);
  rows = rows.map(row => row.id === 'recCreated' ? { ...row, purpose: '云端新的用途' } : row);
  await page.getByRole('button', { name: '刷新链接列表', exact: true }).click();
  await page.getByText('云端新的用途', { exact: true }).waitFor();
  failRefresh = true;
  await page.getByRole('button', { name: '刷新链接列表', exact: true }).click();
  await page.getByText('云端暂时不可用', { exact: true }).waitFor();
  assert.equal(await tableRows.count(), 1, 'refresh failure preserves cached links');
  await page.getByRole('button', { name: '重试', exact: true }).click();
  await page.getByText('云端暂时不可用', { exact: true }).waitFor({ state: 'hidden' });
  await tableRows.getByRole('button', { name: '删除链接', exact: true }).click();
  const confirm = page.getByRole('dialog', { name: '删除导航入口', exact: true });
  await confirm.getByRole('button', { name: '取消', exact: true }).click();
  assert.equal(writes.length, 3);
  await tableRows.getByRole('button', { name: '删除链接', exact: true }).click();
  await confirm.getByRole('button', { name: '删除入口', exact: true }).click();
  await page.getByText('没有匹配的链接', { exact: true }).waitFor();
  await search.fill('');
  const beforeDrag = rows.map(row => row.id);
  const moved = await tableRows.nth(4).getAttribute('data-link-id');
  const target = await tableRows.first().getAttribute('data-link-id');
  await tableRows.nth(4).locator('.drag-handle').dragTo(tableRows.first(), { targetPosition: { x: 180, y: 2 } });
  await page.getByText('顺序已保存', { exact: true }).waitFor();
  assert.equal(await tableRows.first().getAttribute('data-link-id'), moved);
  assert.deepEqual(writes.at(-1).body, { record_id: moved, target_id: target, placement: 'before' });
  assert.deepEqual(rows.filter(row => row.id !== moved).map(row => row.id), beforeDrag.filter(id => id !== moved));
  await page.reload();
  await tableRows.first().waitFor();
  assert.equal(await tableRows.first().getAttribute('data-link-id'), moved, 'order survives reopening');

  // A held pointer crosses pagination even though its source row is unmounted.
  await page.getByRole('button', { name: '下一页', exact: true }).click();
  const crossId = await tableRows.nth(3).getAttribute('data-link-id');
  const handle = tableRows.nth(3).locator('.drag-handle');
  await handle.scrollIntoViewIfNeeded();
  const sourceBox = await handle.boundingBox();
  await page.mouse.move(sourceBox.x + 16, sourceBox.y + 16);
  await page.mouse.down();
  await page.mouse.move(sourceBox.x - 15, sourceBox.y + 16, { steps: 5 });
  await page.locator('.drag-pagination').waitFor();
  const previous = page.getByRole('button', { name: '上一页', exact: true });
  const pagerBox = await previous.boundingBox();
  assert.ok(pagerBox.y >= 0 && pagerBox.y < 1000, 'pagination stays visible while dragging');
  await page.mouse.move(pagerBox.x + 20, pagerBox.y + 16, { steps: 10 });
  await page.mouse.move(pagerBox.x + 21, pagerBox.y + 16);
  await page.waitForFunction(() => document.querySelector('.table-footer b')?.textContent?.trim().startsWith('1 /'));
  await page.locator('.drag-pagination').waitFor();
  await tableRows.first().scrollIntoViewIfNeeded();
  const crossTarget = await tableRows.first().getAttribute('data-link-id');
  const targetBox = await tableRows.first().boundingBox();
  await page.mouse.move(targetBox.x + 150, targetBox.y + 3, { steps: 10 });
  await page.mouse.move(targetBox.x + 151, targetBox.y + 3);
  await page.screenshot({ path: path.join(output, 'drag-cross-page.png'), fullPage: false });
  await page.mouse.up();
  await page.getByText('顺序已保存', { exact: true }).waitFor();
  assert.equal(await tableRows.first().getAttribute('data-link-id'), crossId);
  assert.deepEqual(writes.at(-1).body, { record_id: crossId, target_id: crossTarget, placement: 'before' });

  await page.getByRole('button', { name: '网页导航', exact: true }).click();
  const hiddenOrder = rows.filter(row => row.category !== '网页导航').map(row => row.id);
  const webId = await tableRows.last().getAttribute('data-link-id');
  await tableRows.last().locator('.drag-handle').dragTo(tableRows.first(), { targetPosition: { x: 180, y: 2 } });
  await page.getByText('顺序已保存', { exact: true }).waitFor();
  assert.equal(await tableRows.first().getAttribute('data-link-id'), webId, JSON.stringify({lastWrite: writes.at(-1), webId}));
  assert.deepEqual(rows.filter(row => row.category !== '网页导航').map(row => row.id), hiddenOrder, 'filtered drag preserves hidden rows');
  const orderBeforeFailure = rows.map(row => row.id);
  failSave = true;
  await tableRows.first().locator('.drag-handle').dragTo(tableRows.last(), { targetPosition: { x: 180, y: 30 } });
  await page.locator('.order-status.failed').waitFor();
  assert.deepEqual(rows.map(row => row.id), orderBeforeFailure);
  assert.equal(await tableRows.first().getAttribute('data-link-id'), webId, 'failed save does not claim reordered state');
  const keyboardId = await tableRows.nth(1).getAttribute('data-link-id');
  await tableRows.nth(1).locator('.drag-handle').press('ArrowUp');
  await page.getByText('顺序已保存', { exact: true }).waitFor();
  assert.equal(await tableRows.first().getAttribute('data-link-id'), keyboardId);
  const writeCount = writes.length;
  await tableRows.first().locator('.drag-handle').dragTo(tableRows.first(), { targetPosition: { x: 180, y: 2 } });
  assert.equal(writes.length, writeCount, 'dropping on itself does not submit');
  const cancelBox = await tableRows.first().locator('.drag-handle').boundingBox();
  await page.mouse.move(cancelBox.x + 16, cancelBox.y + 16);
  await page.mouse.down();
  await page.mouse.move(cancelBox.x - 15, cancelBox.y + 16, { steps: 4 });
  await page.locator('.link-drag-preview').waitFor();
  await page.keyboard.press('Escape');
  await page.mouse.up();
  assert.equal(await page.locator('.link-drag-preview').count(), 0);
  assert.equal(writes.length, writeCount, 'Escape cancels drag without writing');
  await page.screenshot({ path: path.join(output, 'reordered.png'), fullPage: true });
  admin = false;
  warning = '云端目录暂时无法读取，正在显示本地已保存的链接。';
  await page.reload();
  await tableRows.first().waitFor();
  await page.getByText(warning, { exact: true }).waitFor();
  assert.equal(await tableRows.count(), 25);
  assert.equal(await page.getByRole('button', { name: '新增链接', exact: true }).count(), 0);
  assert.equal(await page.getByRole('button', { name: '编辑链接', exact: true }).count(), 0);
  assert.equal(await page.getByRole('button', { name: '删除链接', exact: true }).count(), 0);
  assert.equal(await page.locator('.drag-handle').count(), 0);
  for (const width of [1920, 1366, 1024]) {
    await page.setViewportSize({ width, height: 1000 });
    assert.ok(await page.locator('.link-directory-page').evaluate(el => el.scrollWidth <= el.clientWidth + 1));
  }
  assert.deepEqual(errors, []);
  console.log(JSON.stringify({ ok: true, reads, writes: writes.length, screenshots: output, tested: '332 links, CRUD, same-page/cross-page/filtered dragging, hidden order preservation, keyboard reorder, Escape cancellation, failed reorder, reload persistence, readonly permissions, 3 PC widths' }));
} catch (e) { await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true }); throw e; }
finally { clearTimeout(watchdog); await browser.close(); await new Promise(resolve => server.httpServer.close(resolve)); }
