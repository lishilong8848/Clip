import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const dist = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../dist');
const server = http.createServer(async (req, res) => {
  try {
    const file = path.resolve(dist, '.' + (req.url === '/' ? '/assistant.html' : new URL(req.url, 'http://fixture').pathname));
    assert(file.startsWith(dist + path.sep));
    let bytes = await readFile(file);
    if (file.endsWith('assistant.html')) bytes = Buffer.from(bytes.toString().replace('id="clipflow-lighthouse-widget"', 'id="clipflow-lighthouse-widget" data-user-id="key-fixture" data-user-name="Fixture"'));
    res.setHeader('Content-Type', file.endsWith('.js') ? 'application/javascript' : file.endsWith('.css') ? 'text/css' : 'text/html; charset=utf-8');
    res.end(bytes);
  } catch { res.writeHead(404); res.end(); }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const base = 'http://127.0.0.1:' + server.address().port;
const browser = await chromium.launch({ headless: true });
try {
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  const metadata = { id: 'default', name: '测试模型', model: 'fixture-model', endpoint: 'https://fixture.example/v1/chat/completions', configured: true };
  const settings = { models: [metadata], active_model_id: 'default', configured: true, enabled: true };
  let release, captured;
  const saveGate = new Promise(resolve => { release = resolve; });
  await context.route(base + '/api/**', async route => {
    const url = new URL(route.request().url());
    let data = {};
    if (url.pathname === '/api/assistant/settings') {
      if (route.request().method() === 'PUT') {
        captured = route.request().postDataJSON();
        assert.equal(captured.action, 'upsert');
        await saveGate;
      } else assert.equal(route.request().method(), 'GET');
      data = settings;
    } else {
      assert.equal(route.request().method(), 'GET', 'No business writes in this fixture');
      if (url.pathname === '/api/assistant/appearance') data = { color: 'encre', size: 56, shape: 'cercle', snap_back: true };
      if (url.pathname === '/api/assistant/conversation') data = { turns: [], can_manage_settings: true, configured: true, enabled: true, busy: false };
    }
    await route.fulfill({ json: { ok: true, data } });
  });
  const page = await context.newPage(), errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.goto(base);
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  await page.getByRole('button', { name: '模型设置', exact: true }).click();
  await page.getByRole('button', { name: '编辑', exact: true }).click();
  const key = page.getByLabel('API Key', { exact: true });
  assert.equal(await key.inputValue(), '', 'stored credentials are never filled into the browser');
  assert.equal(await key.getAttribute('type'), 'password');
  await key.fill('synthetic-new-key');
  const protectedEvents = await key.evaluate(el => ['copy','cut','contextmenu','dragstart'].map(type => {
    const event = new Event(type, { bubbles: true, cancelable: true });
    el.dispatchEvent(event); return event.defaultPrevented;
  }));
  assert(protectedEvents.every(Boolean), 'copy, cut, context menu and drag must be blocked');
  const pasted = await key.evaluate(el => {
    const event = new Event('paste', { bubbles: true, cancelable: true });
    el.dispatchEvent(event); return !event.defaultPrevented;
  });
  assert(pasted, 'users can still paste a replacement key');
  await page.getByRole('button', { name: '保存', exact: true }).click();
  await page.waitForFunction(() => document.querySelector('.profile-form input[type=password]')?.disabled);
  assert.equal(await page.getByRole('button', { name: '添加模型', exact: true }).isEnabled(), false);
  assert.equal(await page.getByRole('button', { name: '编辑', exact: true }).isEnabled(), false);
  release();
  await page.waitForFunction(() => {
    const input = document.querySelector('.profile-form input[type=password]');
    return input && !input.disabled && input.value === '';
  });
  assert.equal(captured.profile.api_key, 'synthetic-new-key');
  assert(!JSON.stringify(settings).includes('synthetic-new-key'));
  assert.deepEqual(errors, []);
  console.log('[AssistantKeyBrowser] OK: write-only stored credentials, protected replacement input, locked save, cleared key. No business writes.');
} finally {
  await browser.close();
  await new Promise(resolve => server.close(resolve));
}
