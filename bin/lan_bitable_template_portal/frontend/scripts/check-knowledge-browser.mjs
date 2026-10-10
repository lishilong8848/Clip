import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/knowledge');
await mkdir(output, { recursive: true });
const server = await preview({ root, logLevel: 'error', preview: { host: '127.0.0.1', port: 0 } });
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
page.setDefaultTimeout(12000);
const errors = [], requests = [];
let removed = false, revision = 4;
const document = () => ({ id: 'guide', name: '公司出差管理办法.txt', owner_name: '隔离测试', active_version: 2,
  version: revision, can_edit: true, status: removed ? 'deleted' : 'ready', updated_at: 1791510000, size: 1600, chunks: 42 });
const section = ordinal => ({ ordinal, location: `第${ordinal + 1}段`, text: `出差前提交申请，并保留真实报销凭证。隔离测试资料${ordinal}。` });
page.on('pageerror', error => errors.push(error.message));
await page.route('**/api/**', async route => {
  const req = route.request(), url = new URL(req.url()); requests.push([req.method(), url.pathname, url.search]);
  const ok = data => route.fulfill({ json: { ok: true, data } });
  if (url.pathname === '/api/auth/status') return ok({ logged_in: true, user: { open_id: 'knowledge-fixture', role: 'admin', name: '隔离测试' }, scope_options: [{ value: 'E', label: 'E楼' }] });
  if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-knowledge' } });
  if (url.pathname === '/api/assistant/conversation') return ok({ conversation_id: 'fixture', enabled: true, configured: false, turns: [], can_manage_settings: true });
  if (url.pathname === '/api/assistant/appearance') return ok({ color: '#171717', size: 88, shape: 'circle', snap_back: true });
  if (url.pathname === '/api/assistant/knowledge') return ok({ items: (url.searchParams.get('deleted') === '1') === removed ? [document()] : [], total: 1, page: 1, page_size: 20, is_admin: true, settings: { configured: true }, revision });
  if (url.pathname.endsWith('/knowledge/search')) return ok({ mode: 'hybrid', warning: '', items: [{ ...section(0), name: document().name, document_id: 'guide', version: 2, url: '/knowledge-base?document=guide&version=2&chunk=0' }] });
  if (url.pathname === '/api/assistant/knowledge/documents/guide') {
    if (req.method() === 'DELETE') { assert.equal(req.postDataJSON().version, 4); removed = true; revision++; return ok(document()); }
    assert.equal(url.searchParams.get('version'), '2', 'preview uses content version, not optimistic revision');
    const p = Number(url.searchParams.get('page') || 1);
    return ok({ document: { ...document(), view_version: 2 }, sections: Array.from({ length: p === 3 ? 2 : 20 }, (_, i) => section((p - 1) * 20 + i)), page: p, page_size: 20, total: 42 });
  }
  if (url.pathname.endsWith('/knowledge/documents/guide/restore')) { assert.equal(req.postDataJSON().version, 5); removed = false; revision++; return ok(document()); }
  return ok({});
});
try {
  await page.goto(origin + '/knowledge-base');
  await page.getByText('公司出差管理办法.txt', { exact: true }).first().waitFor();
  await page.locator('.kb-row-main').click();
  await page.locator('.kb-section').first().waitFor();
  assert.equal(await page.locator('.kb-section').count(), 20);
  await page.getByRole('button', { name: '下一页片段' }).click();
  await page.waitForFunction(() => document.querySelector('.kb-section')?.textContent.includes('第21段'));
  await page.screenshot({ path: path.join(output, 'knowledge-page.png'), fullPage: true });
  await page.getByRole('button', { name: '移入回收站', exact: true }).click();
  const dialog = page.getByRole('dialog', { name: '移入回收站' });
  await dialog.getByRole('button', { name: '确认', exact: true }).click();
  await page.locator('.kb-detail').waitFor({ state: 'hidden' });
  await page.getByRole('button', { name: '回收站', exact: true }).click();
  await page.getByRole('button', { name: '恢复', exact: true }).click();
  await page.getByRole('dialog', { name: '恢复文档' }).getByRole('button', { name: '确认', exact: true }).click();
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
  const panel = page.locator('.assistant-panel');
  await page.waitForTimeout(450);
  const before = await panel.boundingBox();
  assert.ok(before.width >= 760 && before.height >= 750, JSON.stringify(before));
  assert.equal(await page.getByRole('button', { name: '展开会话', exact: true }).count(), 0);
  const handle = page.locator('.panel-resize');
  await handle.focus();
  await handle.press('Shift+ArrowLeft');
  await page.waitForTimeout(400);
  let resized = await panel.boundingBox();
  assert.ok(Math.abs(resized.width - (before.width - 40)) < 3);
  const grip = await handle.boundingBox();
  await page.mouse.move(grip.x + grip.width / 2, grip.y + grip.height / 2);
  await page.mouse.down();
  await page.mouse.move(grip.x + grip.width / 2 + 48, grip.y + grip.height / 2 - 32, { steps: 12 });
  await page.mouse.up();
  await page.waitForTimeout(400);
  const dragged = await panel.boundingBox();
  assert.ok(Math.abs(dragged.width - (resized.width - 48)) < 3);
  assert.ok(Math.abs(dragged.height - (resized.height - 32)) < 3);
  resized = dragged;
  await page.getByRole('button', { name: '侧边栏（通告待办 / 知识库）' }).click();
  await page.getByRole('tab', { name: '知识库', exact: true }).click();
  await page.getByRole('searchbox', { name: '检索知识库' }).fill('公司出差');
  await page.locator('.lk-result').waitFor();
  assert.equal(await page.locator('.assistant-sidebar.expanded').count(), 1);
  assert.equal(await page.locator('.notice-panel:visible').count(), 0);
  await page.screenshot({ path: path.join(output, 'assistant-knowledge.png'), fullPage: true });
  await page.getByRole('button', { name: '收起助手', exact: true }).click();
  await page.reload();
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  await page.getByRole('textbox', { name: '询问灯塔助手' }).waitFor();
  await page.waitForTimeout(450);
  assert.ok(Math.abs((await panel.boundingBox()).width - resized.width) < 3, 'preferred size survives reload and sidebar close');
  for (const width of [1366, 1920]) {
    await page.setViewportSize({ width, height: 900 });
    await page.waitForTimeout(450);
    const box = await page.locator('.assistant-shell').boundingBox();
    assert.ok(box.x >= 0 && box.y >= 0 && box.x + box.width <= width + 1 && box.y + box.height <= 901, JSON.stringify(box));
  }
  assert.deepEqual(errors, []);
  console.log(JSON.stringify({ ok: true, screenshots: output, checks: 'preview version/pagination, delete/restore, single large resizeable window, persistence, mutually exclusive sidebar, PC bounds' }));
} catch (error) {
  await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true }); throw error;
} finally {
  await browser.close(); await new Promise(resolve => server.httpServer.close(resolve));
}
