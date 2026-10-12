import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/knowledge');
await mkdir(output, { recursive: true });
// Exercise the production build via vite preview so the test matches the actual
// dist bundle shipped to users (keep in sync with source). Build is produced by
// Codex before running; do not point this at the source-only dev server.
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
let mode = 'single';
let multiSeq = 0;
const multiIds = Array.from({ length: 23 }, (_, i) => `m${i + 1}`);
const multiGroups = [
  { id: 'gA', name: '财务组', owner_name: '隔离测试', created_at: 1791510000, count: 0, can_manage: true },
  { id: 'gB', name: '法务组', owner_name: '隔离测试', created_at: 1791510001, count: 0, can_manage: true },
];
const multiAssign = new Map();
multiIds.slice(0, 5).forEach(id => multiAssign.set(id, 'gA'));
multiIds.slice(5, 10).forEach(id => multiAssign.set(id, 'gB'));
const multiDoc = id => {
  const gid = multiAssign.get(id);
  const g = gid ? multiGroups.find(x => x.id === gid) : null;
  return { id, name: `测试文件${id}.txt`, owner_name: '隔离测试', active_version: 1, version: 1, can_edit: true,
    status: 'ready', updated_at: 1791510000, size: 1200, chunks: 3, group_id: g?.id || null, group_name: g?.name || null };
};
const multiCounts = () => {
  const counts = {};
  for (const [id, gid] of multiAssign) counts[gid] = (counts[gid] || 0) + 1;
  return counts;
};
page.on('pageerror', error => errors.push(error.message));
await page.route('**/api/**', async route => {
  const req = route.request(), url = new URL(req.url()); requests.push([req.method(), url.pathname, url.search]);
  // Dev server serves source modules under /src/api/... — never mock those, let Vite serve them.
  if (!url.pathname.startsWith('/api/')) return route.continue();
  const ok = data => route.fulfill({ json: { ok: true, data } });
  if (url.pathname === '/api/auth/status') return ok({ logged_in: true, user: { open_id: 'knowledge-fixture', role: 'admin', name: '隔离测试' }, scope_options: [{ value: 'E', label: 'E楼' }] });
  if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-knowledge' } });
  if (url.pathname === '/api/assistant/conversation') return ok({ conversation_id: 'fixture', enabled: true, configured: false, turns: [], can_manage_settings: true });
  if (url.pathname === '/api/assistant/appearance') return ok({ color: '#171717', size: 88, shape: 'circle', snap_back: true });
  if (mode === 'multi' && url.pathname === '/api/assistant/knowledge/groups') {
    if (req.method() === 'POST') {
      const g = { id: 'g_' + (++multiSeq), name: req.postDataJSON().name, owner_name: '隔离测试', created_at: 1791510002, count: 0, can_manage: true };
      multiGroups.push(g);
      return ok(g);
    }
    const counts = multiCounts();
    const groupsPayload = multiGroups.map(g => ({ ...g, count: counts[g.id] || 0, can_manage: true }));
    return ok({ groups: groupsPayload, total: groupsPayload.length });
  }
  if (mode === 'multi' && url.pathname === '/api/assistant/knowledge/groups/ungrouped/documents' && req.method() === 'PUT') {
    for (const id of req.postDataJSON().document_ids || []) multiAssign.delete(id);
    return ok({ updated: (req.postDataJSON().document_ids || []).length });
  }
  if (mode === 'multi') {
    let m;
    if ((m = url.pathname.match(/^\/api\/assistant\/knowledge\/groups\/([^/]+)\/documents$/)) && req.method() === 'PUT') {
      const gid = decodeURIComponent(m[1]);
      for (const id of req.postDataJSON().document_ids || []) multiAssign.set(id, gid);
      return ok({ updated: (req.postDataJSON().document_ids || []).length });
    }
    if ((m = url.pathname.match(/^\/api\/assistant\/knowledge\/groups\/([^/]+)$/))) {
      const gid = decodeURIComponent(m[1]);
      const g = multiGroups.find(x => x.id === gid);
      if (!g) return route.fulfill({ status: 404, contentType: 'application/json', body: JSON.stringify({ ok: false, error: 'not found' }) });
      if (req.method() === 'PATCH') { g.name = req.postDataJSON().name; return ok(g); }
      if (req.method() === 'DELETE') {
        for (const [id, gid2] of [...multiAssign]) if (gid2 === gid) multiAssign.delete(id);
        const idx = multiGroups.indexOf(g); if (idx >= 0) multiGroups.splice(idx, 1);
        return ok({ deleted: true });
      }
    }
  }
  if (url.pathname === '/api/assistant/knowledge') {
    if (mode === 'multi') {
      const group = url.searchParams.get('group');
      const q = (url.searchParams.get('q') || '').toLowerCase();
      const ids = multiIds.filter(id => {
        const doc = multiDoc(id);
        if (q && !doc.name.toLowerCase().includes(q)) return false;
        if (group === 'ungrouped') return !multiAssign.has(id);
        if (group && group !== 'ungrouped') return multiAssign.get(id) === group;
        return true;
      });
      const pageNum = Number(url.searchParams.get('page') || 1);
      const pageItems = ids.slice((pageNum - 1) * 20, pageNum * 20).map(multiDoc);
      return ok({ items: pageItems, total: ids.length, page: pageNum, page_size: 20, is_admin: true, revision });
    }
    return ok({ items: (url.searchParams.get('deleted') === '1') === removed ? [document()] : [], total: 1, page: 1, page_size: 20, is_admin: true, revision });
  }
  if (url.pathname.endsWith('/knowledge/search')) return ok({ mode: 'keyword', warning: '', items: [{ ...section(0), name: document().name, document_id: 'guide', version: 2, url: '/knowledge-base?document=guide&version=2&chunk=0' }] });
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
  await page.getByText('本地全文检索', { exact: true }).waitFor();
  assert.equal(await page.getByRole('button', { name: '知识库设置', exact: true }).count(), 0, 'KB settings button must not exist');
  assert.equal(await page.getByLabel('API Key', { exact: true }).count(), 0, 'no separate API key config UI');
  assert.equal(await page.getByLabel('嵌入服务端点（Embeddings）', { exact: true }).count(), 0, 'no separate embedding endpoint config UI');
  assert.equal(await page.getByLabel('允许接收文档片段的目标端（每行一个 https 来源）', { exact: true }).count(), 0, 'no separate domain/whitelist config UI');
  await page.screenshot({ path: path.join(output, 'knowledge-no-settings.png'), fullPage: true, animations: 'disabled' });
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
  // ---- Multi-mode: pagination + grouping (separate isolated fixture, never touches real data) ----
  mode = 'multi';
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto(origin + '/knowledge-base');
  await page.getByText('测试文件m1.txt', { exact: true }).waitFor();
  // The assistant panel auto-opens on this page and can intercept clicks on the filter bar.
  async function collapseAssistant() {
    const toggler = page.getByRole('button', { name: '收起助手', exact: true });
    for (let i = 0; i < 15; i++) {
      if (await toggler.count()) { await toggler.click(); await toggler.waitFor({ state: 'hidden' }); return; }
      await page.waitForTimeout(300);
    }
  }
  await collapseAssistant();
  assert.equal(await page.locator('.kb-row').count(), 20, 'all/page1 shows 20 rows');
  assert.equal((await page.locator('.kb-total').textContent()).trim(), '共 23 项');
  for (const label of ['首页', '上一页', '第 1 页', '第 2 页', '下一页', '末页']) {
    assert.equal(await page.getByRole('button', { name: label, exact: true }).count(), 1, `pager control missing: ${label}`);
  }
  await page.getByRole('button', { name: '末页', exact: true }).click();
  await page.getByText('测试文件m23.txt', { exact: true }).waitFor();
  assert.equal(await page.locator('.kb-row').count(), 3, 'last page shows 3 rows');
  await page.screenshot({ path: path.join(output, 'knowledge-pagination.png'), fullPage: true });
  await page.getByRole('button', { name: '第 1 页', exact: true }).click();
  await page.getByText('测试文件m1.txt', { exact: true }).waitFor();
  await collapseAssistant(); // ensure later auto-open cannot block the filter bar
  // group filters
  await page.getByRole('button', { name: /^财务组/ }).first().click();
  await page.getByText('测试文件m1.txt', { exact: true }).waitFor();
  assert.equal(await page.locator('.kb-row').count(), 5, '财务组 shows 5 rows');
  await page.getByRole('button', { name: '未分组', exact: true }).click();
  await page.getByText('测试文件m11.txt', { exact: true }).waitFor();
  assert.equal(await page.locator('.kb-row').count(), 13, '未分组 shows 13 rows (single page, no pager footer)');
  await page.getByRole('button', { name: '全部', exact: true }).click();
  await page.getByText('测试文件m1.txt', { exact: true }).waitFor();
  // group management: create + rename
  await page.getByRole('button', { name: '管理分组', exact: true }).click();
  await page.getByRole('group', { name: '分组管理' }).waitFor();
  await page.getByLabel('新分组名称').fill('临时组');
  await page.getByRole('button', { name: '新建', exact: true }).click();
  await page.locator('.kb-groups-list li').filter({ hasText: '临时组' }).waitFor();
  const tempRow = page.locator('.kb-groups-list li').filter({ hasText: '临时组' });
  await tempRow.getByRole('button', { name: '重命名分组', exact: true }).click();
  await tempRow.locator('input[aria-label^="重命名分组"]').fill('临时组改');
  await tempRow.getByRole('button', { name: '确认重命名', exact: true }).click();
  await page.locator('.kb-groups-list li').filter({ hasText: '临时组改' }).waitFor();
  await page.getByRole('button', { name: '管理分组', exact: true }).click(); // close panel
  // batch selection: select all current page (20 on 全部/page1)
  await page.getByRole('checkbox', { name: '全选本页', exact: true }).click();
  const batchBar = page.getByRole('toolbar', { name: '批量分组' });
  await batchBar.waitFor();
  const batchText = await batchBar.textContent();
  assert.ok(batchText.includes('已选 20 个文件'), `batch bar text: ${batchText}`);
  await batchBar.getByLabel('目标分组').selectOption({ label: '临时组改' });
  await batchBar.getByRole('button', { name: '应用分组', exact: true }).click();
  await page.getByRole('button', { name: /^临时组改/ }).first().click();
  await page.getByText('测试文件m1.txt', { exact: true }).waitFor();
  assert.equal(await page.locator('.kb-row').count(), 20, '临时组改 shows 20 assigned rows');
  await page.screenshot({ path: path.join(output, 'knowledge-groups.png'), fullPage: true });
  // delete the group: files return to ungrouped, group button/panel remove it
  await page.getByRole('button', { name: '管理分组', exact: true }).click();
  const tempRow2 = page.locator('.kb-groups-list li').filter({ hasText: '临时组改' });
  await tempRow2.getByRole('button', { name: '删除分组', exact: true }).click();
  await page.getByRole('dialog', { name: '删除分组' }).getByRole('button', { name: '确认', exact: true }).click();
  await page.getByText('测试文件m1.txt', { exact: true }).waitFor();
  await page.waitForFunction(() => { const t = document.querySelector('.kb-total'); return t && t.textContent.includes('共 23 项'); });
  assert.equal(await page.getByRole('button', { name: /临时组改/ }).count(), 0, 'deleted group filter is removed');
  assert.deepEqual(errors, []);
  console.log(JSON.stringify({ ok: true, screenshots: output, checks: 'no embedding/api-key/domain settings UI, local full-text badge, preview version/pagination, delete/restore, single large resizeable window, persistence, mutually exclusive sidebar, PC bounds, first/last/nearby pagination, group filters/create/rename/delete, current-page batch group assignment' }));
} catch (error) {
  await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true }); throw error;
} finally {
  await browser.close(); await server.close();
}
