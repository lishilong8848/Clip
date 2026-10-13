import assert from 'node:assert/strict';
import http from 'node:http';
import { mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';

// Isolated built assets and synthetic APIs; never contacts a model or Feishu.
const root = path.resolve(process.argv[2] || '../dist');
const output = path.resolve('../../../output/playwright/assistant-tools');
await mkdir(output, { recursive: true });
const server = http.createServer(async (request, response) => {
  try {
    const pathname = new URL(request.url, 'http://localhost').pathname;
    const file = path.resolve(root, '.' + (pathname === '/' ? '/assistant.html' : decodeURIComponent(pathname)));
    if (!file.startsWith(root + path.sep)) throw new Error('outside assets');
    let body = await readFile(file);
    if (file.endsWith('assistant.html')) body = Buffer.from(body.toString().replace('id="clipflow-lighthouse-widget"', 'id="clipflow-lighthouse-widget" data-user-id="tools-fixture" data-user-name="测试账号"'));
    response.setHeader('Content-Type', file.endsWith('.js') ? 'application/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html; charset=utf-8');
    response.end(body);
  } catch { response.writeHead(404); response.end(); }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const base = 'http://127.0.0.1:' + server.address().port;
const browser = await chromium.launch({ headless: true });
const errors = [], writes = [];
try {
  for (const [width, admin] of [[1440, true], [390, false]]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    let revision = 0;
    const builtin = { id: 'lighthouse-utilities', name: '内置常用MCP', transport: 'builtin', enabled: true,
      removable: false, admin_only: false, allowed_tools: ['text_lines'], tools: [{ name: 'text_lines', description: '文本去重排序' }] };
    let servers = [builtin];
    const state = () => ({ revision, can_manage: admin, servers, native_tools: [{ name: 'public_search', label: '联网搜索', description: '搜索公开资料并保留来源' }], recent_calls: [] });
    await context.route(base + '/api/**', async route => {
      const pathname = new URL(route.request().url()).pathname, method = route.request().method();
      const send = data => route.fulfill({ contentType: 'application/json', body: JSON.stringify({ ok: true, data }) });
      if (pathname.endsWith('/auth/status')) return send({ logged_in: true, user: { open_id: 'tools-fixture', modules: ['assistant'] } });
      if (pathname.endsWith('/conversation')) return send({ turns: [], conversation_id: 'tools-conversation', configured: true, enabled: true, can_manage_settings: true, models: [], busy: false });
      if (pathname.endsWith('/appearance')) return send({ color: 'encre', shape: 'cercle', size: 56, snap_back: true });
      if (pathname.endsWith('/skills')) return send({ items: [] });
      if (pathname.endsWith('/extensions')) {
        if (method === 'PUT') {
          assert(admin, 'ordinary people cannot edit tools');
          const payload = route.request().postDataJSON(); writes.push(payload);
          assert.equal(payload.revision, revision++);
          if (payload.id === builtin.id) builtin.enabled = payload.enabled;
          else servers.push({ ...payload, id: 'a'.repeat(32), tools: [], allowed_tools: [], removable: true, enabled: false });
        }
        return send(state());
      }
      if (pathname.endsWith('/test')) return send({ tools: builtin.tools });
      return send({});
    });
    const page = await context.newPage(); page.on('pageerror', error => errors.push(error.message));
    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    await page.getByRole('button', { name: '技能与工具', exact: true }).click();
    await page.getByRole('button', { name: '工具与MCP', exact: true }).click();
    await page.getByRole('heading', { name: 'MCP服务', exact: true }).waitFor();
    assert(await page.getByText('内置常用MCP', { exact: true }).isVisible());
    if (admin) {
      await page.getByLabel('启用内置常用MCP', { exact: true }).uncheck();
      await page.getByText('已停用', { exact: true }).waitFor();
      await page.getByRole('button', { name: '添加MCP', exact: true }).click();
      await page.getByLabel('名称', { exact: true }).fill('测试搜索MCP');
      await page.getByLabel('MCP服务地址', { exact: true }).fill('https://example.com/mcp');
      await page.getByLabel('认证密钥', { exact: true }).fill('fixture-key');
      await page.getByRole('button', { name: '保存', exact: true }).click();
      await page.getByRole('status').filter({ hasText: '连接已保存' }).waitFor();
      assert.equal(await page.getByLabel('认证密钥', { exact: true }).inputValue(), '');
    } else {
      assert.equal(await page.getByRole('button', { name: '添加MCP', exact: true }).count(), 0);
      assert.equal(await page.getByRole('button', { name: /测试内置/ }).count(), 0);
    }
    assert(await page.locator('.extension-manager').evaluate(element => element.scrollWidth <= element.clientWidth + 1));
    const mismatches = await page.evaluate(() => [...document.querySelectorAll('.extension-manager label[for]')].filter(label => !document.getElementById(label.htmlFor)).map(label => label.htmlFor));
    assert.deepEqual(mismatches, []);
    await page.screenshot({ path: path.join(output, `tools-${width}.png`) });
    await context.close();
  }
  assert.deepEqual(errors, []);
  console.log('[ToolsBrowser] OK: admin configuration, masked secret, member view, PC/mobile bounds and labels. Synthetic APIs only.');
} finally { await browser.close(); await new Promise(resolve => server.close(resolve)); }
