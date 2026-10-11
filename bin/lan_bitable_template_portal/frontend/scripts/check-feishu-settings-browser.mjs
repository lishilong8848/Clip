import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/feishu-settings');
await mkdir(output, { recursive: true });
const server = await preview({ root, logLevel: 'error', preview: { host: '127.0.0.1', port: 0 } });
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
const browser = await chromium.launch({ headless: true });
const errors = [], writes = [];
let config = { app_id: 'cli_existingfixture', enabled: true, has_secret: true, revision: 'r1', restart_required: false };
let rejectSave = false;
async function open(role) {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  page.setDefaultTimeout(12000);
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/api/**', async route => {
    const req = route.request(), url = new URL(req.url());
    const ok = data => route.fulfill({ json: { ok: true, data } });
    if (url.pathname === '/api/auth/status') return ok({ logged_in: true, user: { open_id: 'settings-fixture', role, name: '隔离测试' }, scope_options: [{ value: 'E', label: 'E楼' }] });
    if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-feishu-settings' } });
    if (url.pathname === '/api/assistant/conversation') return ok({ enabled: true, configured: false, turns: [] });
    if (url.pathname === '/api/assistant/knowledge') return ok({ items: [], total: 0 });
    if (url.pathname === '/api/assistant/feishu-settings') {
      assert.equal(role, 'admin', 'non-admin must not mount protected settings');
      if (req.method() === 'POST') {
        const body = req.postDataJSON(); writes.push(body);
        if (rejectSave) return route.fulfill({ status: 409, json: { ok: false, error: '配置已被其他管理员修改，请重新读取后保存。' } });
        assert.equal(body.revision, config.revision);
        config = { ...config, app_id: body.app_id, enabled: body.enabled, has_secret: true, revision: 'r' + (writes.length + 1), restart_required: true };
      }
      return ok(config);
    }
    return ok({});
  });
  await page.goto(origin + '/knowledge-base?admin=feishu');
  return page;
}

let page;
try {
  page = await open('admin');
  const form = page.getByRole('form', { name: '飞书智能体配置' });
  await form.waitFor();
  const id = form.getByLabel('App ID', { exact: true });
  const secret = form.getByLabel('App Secret', { exact: true });
  const save = form.getByRole('button', { name: '保存配置', exact: true });
  await page.waitForFunction(() => document.querySelector('.feishu-settings input[type=text]')?.value === 'cli_existingfixture');
  assert.equal(await secret.inputValue(), '', 'stored secret is never populated');
  assert.equal(await secret.getAttribute('type'), 'password');
  assert.match(await secret.getAttribute('placeholder'), /留空保留/);
  assert.equal(writes.length, 0, 'opening settings is read-only');
  await form.getByLabel('启用飞书智能体长连接').uncheck();
  await save.click();
  await form.getByText('配置已保存，请重启程序后使用新配置。当前会话未中断。', { exact: true }).waitFor();
  assert.equal(writes[0].app_secret, '', 'blank keeps existing secret');
  assert.equal(writes[0].enabled, false);
  await id.fill('cli_changedfixture');
  assert.equal(await save.isDisabled(), true, 'changing app requires its new secret');
  await secret.fill('fixture-only-no-real-key');
  await form.getByLabel('启用飞书智能体长连接').check();
  assert.equal(await save.isEnabled(), true);
  const response = page.waitForResponse(r => r.url().endsWith('/api/assistant/feishu-settings') && r.request().method() === 'POST');
  await save.click(); await response;
  await page.waitForFunction(() => document.querySelector('.feishu-settings input[type=password]')?.value === '');
  assert.equal(writes[1].app_id, 'cli_changedfixture');
  assert.equal(writes[1].app_secret, 'fixture-only-no-real-key');
  await page.screenshot({ path: path.join(output, 'admin-settings.png'), fullPage: true, animations: 'disabled' });
  rejectSave = true;
  await id.fill('cli_unsavedfixture');
  await secret.fill('fixture-unsaved-key');
  await save.click();
  await form.getByRole('alert').waitFor();
  assert.equal(await id.inputValue(), 'cli_unsavedfixture', 'failed save retains entered values');
  assert.equal(await secret.inputValue(), 'fixture-unsaved-key');
  await page.close();
  page = await open('user');
  await page.getByRole('heading', { name: '共享知识库' }).waitFor();
  assert.equal(await page.getByRole('form', { name: '飞书智能体配置' }).count(), 0);
  assert.deepEqual(errors, []);
  console.log(JSON.stringify({ ok: true, checks: 'admin entry, secret masked/never returned, blank retention, changed app needs key, explicit save, restart status, error retains inputs, non-admin hidden', screenshots: output }));
} catch (error) {
  await page?.screenshot({ path: path.join(output, 'failure.png'), fullPage: true }); throw error;
} finally {
  await browser.close();
  await new Promise(resolve => server.httpServer.close(resolve));
}
