import assert from 'node:assert/strict';
import { mkdir, mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'vite';
import vue from '@vitejs/plugin-vue';
import { chromium } from 'playwright';

const root = fileURLToPath(new URL('..', import.meta.url));
const output = path.resolve(root, '../../../output/playwright/account-fixed-duty');
await mkdir(output, { recursive: true });
const cacheDir = await mkdtemp(path.join(tmpdir(), 'account-fixed-duty-vite-'));
const entry = '/__account-fixture.js';
const server = await createServer({ root, configFile: false, cacheDir, logLevel: 'error',
  server: { host: '127.0.0.1', port: 0 }, plugins: [vue(), {
    name: 'isolated-account-fixture',
    resolveId: id => id === entry ? id : null,
    load: id => id === entry ? `import {createApp} from 'vue';
      import Accounts from '/src/components/AdminAccounts.vue';
      createApp(Accounts, {pendingCount: 0, permissions: []}).mount('#app');` : null,
    configureServer(server) {
      server.middlewares.use('/__account-fixture.html', (_req, res) => {
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
        res.end(`<!doctype html><html><head><meta charset="utf-8"></head><body><div id="app"></div><script type="module" src="${entry}"></script></body></html>`);
      });
    },
  }] });
await server.listen();
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
const browser = await chromium.launch({ headless: true });
try {
  for (const [actorLevel, targetLevel, home] of [['L0', 'L1', 'H'], ['L1', 'L1', 'H'],
    ['L2', 'L4', 'H'], ['L2', 'L3', 'H'], ['L2', 'L4', 'A']]) {
    const foreign = home !== 'H';
    const editable = actorLevel === 'L0' || actorLevel === 'L2' && !foreign, writes = [], errors = [];
    const duty = { id: 'account:ou_fixtureH', open_id: 'ou_fixtureH', name: foreign ? '跨楼工程师' : 'H楼固定值班',
      record_id: '', kind: foreign ? 'external' : 'duty', category: foreign ? 'engineer' : 'duty', own_scopes: [home], home_scope: home,
      level: targetLevel, role: targetLevel === 'L1' ? 'admin' : 'building', scopes: [home],
      modules: targetLevel === 'L4' ? ['maintenance'] : ['maintenance', 'accounts'],
      enabled: true, authorized: true, revision: 'fixture-1', locked: false, blocked_reason: '',
      can_edit: editable || foreign, can_edit_access: editable, can_grant_l0: editable || foreign,
      can_delete: false, can_set_password: false, can_impersonate: false, can_access_signature: false };
    const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
    await context.route('**/*', async route => {
      const request = route.request(), url = new URL(request.url());
      if (url.origin !== origin) return route.abort();
      if (!url.pathname.startsWith('/api/')) return route.continue();
      if (url.pathname === '/api/auth/accounts') return route.fulfill({ json: { ok: true, data: {
        items: [duty], policy: { open_id: 'ou_fixtureActor', level: actorLevel, scope_rules: ['H'], modules: ['maintenance', 'accounts'] },
        scope_options: [{ value: 'H', label: 'H楼' }],
        category_options: [{ value: 'duty', label: '固定值班账号' }, { value: 'engineer', label: '工程师' }],
        module_options: [{ value: 'maintenance', label: '维保通告' }, { value: 'accounts', label: '账号管理' }],
        level_options: (actorLevel === 'L2' ? ['L0', 'L3', 'L4'] : ['L0', 'L1', 'L2', 'L3', 'L4'])
          .map(value => ({ value, label: value })),
      } } });
      if (url.pathname === '/api/auth/accounts/permissions' && request.method() === 'POST') {
        writes.push(request.postDataJSON());
        Object.assign(duty, writes.at(-1).changes[0], { revision: 'fixture-2' });
        return route.fulfill({ json: { ok: true, data: { changed: 1 } } });
      }
      errors.push(`Unexpected request: ${request.method()} ${url.pathname}`);
      return route.abort();
    });
    const page = await context.newPage();
    page.setDefaultTimeout(15000);
    page.on('pageerror', error => errors.push(error.message));
    page.on('console', message => { if (message.type() === 'error') errors.push(message.text()); });
    try {
      await page.goto(origin + '/__account-fixture.html');
      await page.getByRole('button', { name: `管理${duty.name}`, exact: true }).click();
      const editor = page.getByRole('dialog', { name: duty.name, exact: true });
      const level = editor.getByLabel('权限级别', { exact: true });
      if (editable) {
        assert(await level.isEnabled());
        const savedLevel = actorLevel === 'L2' ? targetLevel : 'L4';
        if (actorLevel === 'L2') {
          assert.equal(await level.inputValue(), targetLevel);
          assert(await level.locator(`option[value="${targetLevel}"]`).isEnabled());
        } else await level.selectOption(savedLevel);
        await page.screenshot({ path: path.join(output, `${actorLevel}-${targetLevel}.png`), animations: 'disabled' });
        await editor.getByRole('button', { name: '保存权限', exact: true }).click();
        await editor.waitFor({ state: 'hidden' });
        assert.equal(writes.length, 1);
        const change = writes[0].changes[0];
        assert.equal(change.level, savedLevel);
        assert.equal(change.role, 'building');
        assert.deepEqual(change.scopes, ['H']);
        assert.deepEqual(change.modules, savedLevel === 'L4' ? ['maintenance'] : ['maintenance', 'accounts']);
        await page.reload();
        await page.getByRole('button', { name: `管理${duty.name}`, exact: true }).click();
        assert.equal(await level.inputValue(), savedLevel);
      } else if (foreign) {
        assert.equal(await level.inputValue(), 'L4');
        assert(await level.locator('option[value="L4"]').evaluate(option => option.disabled));
        assert.equal(await level.locator('option[value="L3"]').count(), 0);
        assert(await editor.getByRole('button', { name: '保存权限', exact: true }).isDisabled());
        assert(!await editor.getByRole('button', { name: '保存启用状态', exact: true }).isDisabled());
        assert.equal(writes.length, 0);
      } else {
        assert.equal(await level.count(), 0);
        assert(await editor.getByRole('button', { name: '保存启用状态', exact: true }).isDisabled());
        assert.equal(writes.length, 0);
      }
      assert.equal(await page.getByRole('button', { name: '撤销H楼固定值班的登录权限', exact: true }).count(), 0);
      assert.deepEqual(errors, []);
      console.log(`PASS ${actorLevel}/${targetLevel}/${home}: scoped access and legacy engineer boundaries; cloud requests blocked`);
    } catch (error) {
      console.error(errors, await page.locator('body').innerText());
      throw error;
    } finally { await context.close(); }
  }
} finally {
  await browser.close();
  await server.close();
}
