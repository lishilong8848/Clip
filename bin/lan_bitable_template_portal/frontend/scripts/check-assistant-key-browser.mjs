import assert from 'node:assert/strict';
import http from 'node:http';
import { readFile, mkdir } from 'node:fs/promises';
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
  // Old model record deliberately lacks the new capability fields so defaults must kick in.
  const metadata = { id: 'default', name: '测试模型', model: 'fixture-model', endpoint: 'https://fixture.example/v1/chat/completions', configured: true };
  let settings = { models: [metadata], active_model_id: 'default', configured: true, enabled: true };
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
        if (captured.profile) {
          Object.assign(metadata, captured.profile);
          delete metadata.api_key; // the server never echoes credentials back
          settings = { models: [metadata], active_model_id: metadata.id, configured: true, enabled: true };
        }
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

  // Visibility helpers: robust against Vue's async DOM flush.
  const waitVisible = locator => locator.waitFor({ state: 'visible' });
  const assertHidden = async (locator, label, timeout = 3000) => {
    const start = Date.now();
    while (Date.now() - start < timeout) {
      if (!(await locator.isVisible())) return;
      await page.waitForTimeout(30);
    }
    throw new Error('expected hidden: ' + label);
  };

  await page.goto(base);
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  await page.getByRole('button', { name: '模型设置', exact: true }).click();
  await page.getByRole('button', { name: '编辑', exact: true }).click();

  // --- New capability contract: conditional display and defaults on legacy models ---
  const reasoning = page.getByRole('checkbox', { name: '思考模式', exact: true });
  assert.equal(await reasoning.isChecked(), false, 'reasoning defaults off');
  assert.equal(await page.getByRole('checkbox', { name: '工具调用', exact: true }).isChecked(), true, 'tool_calls defaults on');
  assert.equal(await page.getByRole('checkbox', { name: '图片输入', exact: true }).isChecked(), true, 'image_input defaults on');
  assert.equal(await page.getByRole('checkbox', { name: '自定义协议', exact: true }).isChecked(), false, 'custom_protocol defaults off');
  await assertHidden(page.getByRole('checkbox', { name: '仅思考模式', exact: true }), 'reasoning_only hidden while reasoning off');
  await assertHidden(page.getByRole('checkbox', { name: '允许关闭思考', exact: true }), 'allow_reasoning_off hidden while reasoning off');
  await assertHidden(page.getByRole('group', { name: '思考选项' }), 'effort controls hidden while reasoning off');

  // Enable reasoning -> sub controls and effort group appear, default effort is xhigh (极致).
  await reasoning.check();
  await waitVisible(page.getByRole('checkbox', { name: '仅思考模式', exact: true }));
  await waitVisible(page.getByRole('checkbox', { name: '允许关闭思考', exact: true }));
  await waitVisible(page.getByRole('group', { name: '思考选项' }));
  assert.equal(await page.getByRole('checkbox', { name: '极致', exact: true }).isChecked(), true, 'xhigh checked by default');
  assert.equal(await page.getByRole('checkbox', { name: '低', exact: true }).isChecked(), false);
  assert.equal(await page.getByRole('checkbox', { name: '中', exact: true }).isChecked(), false);
  assert.equal(await page.getByRole('checkbox', { name: '高', exact: true }).isChecked(), false);
  const effortSelect = page.getByLabel('默认思考强度');
  assert.equal(await effortSelect.inputValue(), 'xhigh', 'default effort starts as xhigh');
  assert.equal(await page.locator('select[name=reasoning_effort] option[value="off"]').count(), 0, 'off not offered while allow_reasoning_off off');

  // Add 高, then remove 极致 -> default effort must stay legal (becomes 高).
  await page.getByRole('checkbox', { name: '高', exact: true }).check();
  assert.equal(await page.getByRole('checkbox', { name: '高', exact: true }).isChecked(), true);
  assert.equal(await page.getByRole('checkbox', { name: '极致', exact: true }).isChecked(), true);
  await page.getByRole('checkbox', { name: '极致', exact: true }).uncheck();
  assert.equal(await page.getByRole('checkbox', { name: '极致', exact: true }).isChecked(), false);
  assert.equal(await effortSelect.inputValue(), 'high', 'default effort auto-adjusts to a still-selected strength');

  // allow_reasoning_off adds the 关闭 option; removing the capability drops it and reverts the effort.
  await page.getByRole('checkbox', { name: '允许关闭思考', exact: true }).check();
  assert.equal(await page.locator('select[name=reasoning_effort] option[value="off"]').count(), 1, '关闭 offered when allowed');
  await effortSelect.selectOption('off');
  assert.equal(await effortSelect.inputValue(), 'off');
  await page.getByRole('checkbox', { name: '允许关闭思考', exact: true }).uncheck();
  assert.equal(await page.locator('select[name=reasoning_effort] option[value="off"]').count(), 0, '关闭 removed when not allowed');
  assert.equal(await effortSelect.inputValue(), 'high', 'effort reverts when off capability disabled');

  // reasoning_only hides 关闭 even while allow_reasoning_off is enabled.
  await page.getByRole('checkbox', { name: '允许关闭思考', exact: true }).check();
  assert.equal(await page.locator('select[name=reasoning_effort] option[value="off"]').count(), 1);
  await page.getByRole('checkbox', { name: '仅思考模式', exact: true }).check();
  assert.equal(await page.locator('select[name=reasoning_effort] option[value="off"]').count(), 0, '关闭 hidden under reasoning_only');
  assert.equal(await effortSelect.inputValue(), 'high');

  // Disabling thinking clears sub-switches and restores xhigh defaults.
  await page.getByRole('checkbox', { name: '极致', exact: true }).check();   // efforts -> [high, xhigh] (high still selected)
  await page.getByRole('checkbox', { name: '高', exact: true }).uncheck();   // efforts -> [xhigh], effort -> xhigh
  assert.equal(await effortSelect.inputValue(), 'xhigh');
  await reasoning.uncheck();
  assert.equal(await reasoning.isChecked(), false);
  await assertHidden(page.getByRole('checkbox', { name: '仅思考模式', exact: true }), 'reasoning_only cleared and hidden');
  await assertHidden(page.getByRole('checkbox', { name: '允许关闭思考', exact: true }), 'allow_reasoning_off cleared and hidden');
  await assertHidden(page.getByRole('group', { name: '思考选项' }), 'effort group hidden after disabling thinking');
  await reasoning.check();
  await waitVisible(page.getByRole('group', { name: '思考选项' }));
  assert.equal(await page.getByRole('checkbox', { name: '极致', exact: true }).isChecked(), true, 'xhigh restored after re-enable');
  assert.equal(await page.getByRole('checkbox', { name: '低', exact: true }).isChecked(), false);
  assert.equal(await page.getByRole('checkbox', { name: '中', exact: true }).isChecked(), false);
  assert.equal(await page.getByRole('checkbox', { name: '高', exact: true }).isChecked(), false);
  assert.equal(await effortSelect.inputValue(), 'xhigh', 'effort restored to xhigh after re-enable');
  assert.equal(await page.getByRole('checkbox', { name: '仅思考模式', exact: true }).isChecked(), false, 'reasoning_only reset');
  assert.equal(await page.getByRole('checkbox', { name: '允许关闭思考', exact: true }).isChecked(), false, 'allow_reasoning_off reset');

  // Configure the final capability set for the save round-trip.
  await page.getByRole('checkbox', { name: '高', exact: true }).check();            // efforts -> [xhigh, high]
  await page.getByRole('checkbox', { name: '仅思考模式', exact: true }).check();
  await page.getByRole('checkbox', { name: '允许关闭思考', exact: true }).check();
  await page.getByRole('checkbox', { name: '自定义协议', exact: true }).check();
  await page.getByRole('checkbox', { name: '高', exact: true }).uncheck();          // efforts -> [xhigh]
  await page.getByRole('checkbox', { name: '低', exact: true }).check();            // efforts -> [xhigh, low]
  await effortSelect.selectOption('low');

  // --- Original stored-credential behaviour (kept) ---
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

  // --- Saved profile actually carries the capability contract ---
  assert.equal(captured.profile.api_key, 'synthetic-new-key');
  assert.equal(captured.profile.tool_calls, true);
  assert.equal(captured.profile.image_input, true);
  assert.equal(captured.profile.reasoning, true);
  assert.equal(captured.profile.reasoning_only, true);
  assert.equal(captured.profile.allow_reasoning_off, true);
  assert.equal(captured.profile.custom_protocol, true);
  assert.deepEqual(captured.profile.reasoning_efforts, ['xhigh', 'low']);
  assert.equal(captured.profile.reasoning_effort, 'low');

  // --- Re-open settings and read the saved profile back ---
  await page.getByRole('button', { name: '返回会话', exact: true }).click();
  await page.getByRole('button', { name: '模型设置', exact: true }).click();
  await page.getByRole('button', { name: '编辑', exact: true }).click();
  await waitVisible(page.getByRole('group', { name: '思考选项' }));
  assert.equal(await page.getByRole('checkbox', { name: '工具调用', exact: true }).isChecked(), true);
  assert.equal(await page.getByRole('checkbox', { name: '图片输入', exact: true }).isChecked(), true);
  assert.equal(await page.getByRole('checkbox', { name: '思考模式', exact: true }).isChecked(), true);
  assert.equal(await page.getByRole('checkbox', { name: '自定义协议', exact: true }).isChecked(), true);
  assert.equal(await page.getByRole('checkbox', { name: '仅思考模式', exact: true }).isChecked(), true);
  assert.equal(await page.getByRole('checkbox', { name: '允许关闭思考', exact: true }).isChecked(), true);
  assert.equal(await page.getByRole('checkbox', { name: '极致', exact: true }).isChecked(), true);
  assert.equal(await page.getByRole('checkbox', { name: '低', exact: true }).isChecked(), true);
  assert.equal(await page.getByRole('checkbox', { name: '中', exact: true }).isChecked(), false);
  assert.equal(await page.getByRole('checkbox', { name: '高', exact: true }).isChecked(), false);
  assert.equal(await page.getByLabel('默认思考强度').inputValue(), 'low', 'saved default effort reads back');
  assert.equal(await page.locator('select[name=reasoning_effort] option[value="off"]').count(), 0, 'off hidden after reading reasoning_only=true');
  assert.equal(await page.locator('select[name=reasoning_effort] option').count(), 2, 'dropdown offers only the selected strengths');

  assert(!JSON.stringify(settings).includes('synthetic-new-key'));
  assert.deepEqual(errors, []);
  const screenshotDir = path.resolve(dist, '../../../../output/playwright/assistant');
  await mkdir(screenshotDir, { recursive: true });
  await page.getByRole('group', { name: '能力配置', exact: true }).scrollIntoViewIfNeeded();
  await page.screenshot({ path: path.join(screenshotDir, 'model-capabilities.png'), fullPage: true });
  console.log('[AssistantKeyBrowser] OK: capability contract (conditional display, xhigh default, persisted profile, read-back, cleanup) + write-only stored credentials, protected replacement input, locked save, cleared key. No business writes.');
} finally {
  await browser.close();
  await new Promise(resolve => server.close(resolve));
}
