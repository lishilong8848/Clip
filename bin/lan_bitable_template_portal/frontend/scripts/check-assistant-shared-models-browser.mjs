import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile, readdir, mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dist = path.join(root, 'dist');
const output = path.resolve(root, '../../../output/playwright/shared-models');
await mkdir(output, { recursive: true });
let assistantChunk;
for (const name of await readdir(path.join(dist, 'assets'))) {
  if (name.endsWith('.js') && (await readFile(path.join(dist, 'assets', name), 'utf8')).includes('assistant-launcher')) assistantChunk = name;
}
assert(assistantChunk);
const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://fixture');
    const file = path.resolve(dist, '.' + (url.pathname === '/' ? '/assistant.html' : url.pathname));
    assert(file.startsWith(dist + path.sep));
    let bytes = await readFile(file);
    if (file.endsWith('assistant.html')) bytes = Buffer.from(bytes.toString().replace('id="clipflow-lighthouse-widget"', 'id="clipflow-lighthouse-widget" data-user-id="fixture" data-user-name="测试账号"'));
    res.setHeader('Content-Type', file.endsWith('.js') ? 'application/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html; charset=utf-8');
    res.end(bytes);
  } catch { res.writeHead(404); res.end(); }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const base = 'http://127.0.0.1:' + server.address().port;
const browser = await chromium.launch({ headless: true });
const shared = { id: 'shared_team', name: '团队默认模型', model: 'fixture', endpoint: 'https://fixture.example/v1/chat/completions', configured: true, shared: true };
try {
  for (const admin of [false, true]) {
    const context = await browser.newContext({ viewport: { width: 1600, height: 1000 } });
    let models = [{ ...shared }], captured;
    const settings = () => ({ models, active_model_id: shared.id, configured: true, enabled: true, can_manage_shared: admin });
    await context.route(base + '/api/**', async route => {
      const url = new URL(route.request().url());
      let data = {};
      if (url.pathname.endsWith('/settings')) {
        if (route.request().method() === 'PUT') {
          captured = route.request().postDataJSON();
          assert.equal(captured.scope, 'shared');
          assert(admin);
          if (captured.action === 'upsert') {
            const { api_key, ...profile } = captured.profile;
            assert.equal(api_key, 'synthetic-fixture-key');
            models = [...models, { ...profile, shared: true, configured: true }];
          } else if (captured.action === 'delete') models = models.filter(m => m.id !== captured.id);
        }
        data = settings();
      } else {
        assert.equal(route.request().method(), 'GET', 'Never submit business in browser QA');
        if (url.pathname.endsWith('/appearance')) data = { color: 'encre', size: 56, shape: 'cercle', snap_back: true };
        if (url.pathname.endsWith('/conversation')) data = { conversation_id: 'fixture-conversation', can_manage_settings: true,
          enabled: true, configured: true, busy: false, model_options: models, model_id: shared.id, model_name: shared.name,
          turns: [{ operation_id: 'fixture-turn', question: '将完整的所有进行中的通告发给我', answer: '已备齐全部28条通告，确认后发送给本人。',
            status: 'completed', at: 1000, plan: { id: 'fixture-plan', title: '发送完整未结束通告至本人', status: 'awaiting_confirmation',
              can_edit: true, version: 1, operations: [{ api_id: 'POST /api/message-delivery/send', name: '发送飞书消息',
                body: { recipient_ids: ['__self__'], text: '完整28条明细' }, selected_labels: { recipient_names: '本人 · 测试账号 · 工号 1001' } }] } }] };
      }
      await route.fulfill({ json: { ok: true, data } });
    });
    const page = await context.newPage(), errors = [];
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    const confirm = page.getByRole('button', { name: '确认操作清单', exact: true });
    await confirm.waitFor();
    assert(await confirm.isEnabled());
    const color = await confirm.evaluate(el => {
      const canvas = document.createElement('canvas'), ctx = canvas.getContext('2d');
      ctx.fillStyle = getComputedStyle(el).backgroundColor; ctx.fillRect(0, 0, 1, 1);
      return [...ctx.getImageData(0, 0, 1, 1).data];
    });
    assert(color[1] > color[0] + 40, `enabled confirmation must be green, not gray: ${color}`);
    await page.screenshot({ path: path.join(output, admin ? 'admin-confirm.png' : 'user-confirm.png') });
    await page.getByRole('button', { name: '模型设置', exact: true }).click();
    await page.getByText('共享默认', { exact: true }).waitFor();
    assert.equal(await page.getByRole('button', { name: '添加默认模型', exact: true }).count(), admin ? 1 : 0);
    assert.equal(await page.getByRole('button', { name: '编辑', exact: true }).count(), admin ? 1 : 0);
    assert.equal(await page.getByRole('button', { name: '删除', exact: true }).count(), admin ? 1 : 0);
    if (admin) {
      await page.getByRole('button', { name: '添加默认模型', exact: true }).click();
      assert(await page.getByRole('button', { name: '保存', exact: true }).isDisabled());
      await page.getByLabel('显示名称', { exact: true }).fill('新共享模型');
      await page.getByLabel('接口地址', { exact: true }).fill(shared.endpoint);
      await page.getByLabel('模型名称', { exact: true }).fill('fixture-2');
      await page.getByLabel('API Key', { exact: true }).fill('synthetic-fixture-key');
      await page.getByRole('button', { name: '保存', exact: true }).click();
      await page.locator('.model-card').filter({ hasText: '新共享模型' }).waitFor();
      assert.equal(captured.scope, 'shared');
      assert.equal(await page.getByLabel('API Key', { exact: true }).inputValue(), '');
      await page.locator('.model-card').filter({ hasText: '新共享模型' }).getByRole('button', { name: '删除', exact: true }).click();
      await page.getByText(/所有账号将无法再选择此默认模型/).waitFor();
      await page.screenshot({ path: path.join(output, 'delete-shared-confirm.png'), animations: 'disabled' });
      await page.getByRole('button', { name: '确认', exact: true }).click();
      await page.locator('.model-card').filter({ hasText: '新共享模型' }).waitFor({ state: 'detached' });
      assert.equal(captured.action, 'delete');
    } else {
      assert.equal(await page.locator('.profile-form').count(), 0);
      await page.getByRole('button', { name: '添加模型', exact: true }).click();
      await page.getByText('添加个人模型', { exact: true }).waitFor();
    }
    await page.screenshot({ path: path.join(output, admin ? 'admin-settings.png' : 'user-settings.png') });
    assert.deepEqual(errors, []);
    await context.close();
  }
  const context = await browser.newContext({ viewport: { width: 1600, height: 1000 } });
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  await context.route(base + '/assets/' + assistantChunk, async route => { await gate; await route.abort(); });
  const page = await context.newPage();
  await page.goto(base);
  await page.getByRole('button', { name: '正在载入灯塔助手', exact: true }).waitFor();
  release();
  await page.getByRole('button', { name: '助手加载失败，刷新页面重试', exact: true }).waitFor({ timeout: 15000 });
  await page.screenshot({ path: path.join(output, 'entry-retry.png') });
  await context.close();
  console.log('[AssistantSharedModelsBrowser] OK: ordinary/admin controls, confirmation color, private key, visible loading/error entry. No live writes.');
} finally {
  await browser.close();
  await new Promise(resolve => server.close(resolve));
}
