import assert from 'node:assert/strict';
import path from 'node:path';
import { mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { createServer, preview } from 'vite';
import vue from '@vitejs/plugin-vue';
import { chromium } from 'playwright';
import { layoutApiFixture } from './layout-api-fixture.mjs';

const root = fileURLToPath(new URL('..', import.meta.url));
const dist = process.argv.includes('--dist'), workspace = process.argv.includes('--workspace');
const width = Number(process.argv.find(arg => arg.startsWith('--width='))?.split('=')[1] || 1440);
const output = path.resolve(root, '../../../output/playwright/signature-performance', dist ? `dist-${workspace ? 'v2' : 'legacy'}-${width}` : 'source');
await mkdir(output, { recursive: true });
const html = `<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<div id="page-back-slot"></div><div id="app"></div><script type="module">
import {createApp,h,reactive} from 'vue';import Page from '/src/components/SignatureManagementPage.vue';
import UiTransition from '/src/components/UiTransition.vue';
const state=reactive({checking:false,loggedIn:true,userId:'A',loginUrl:'/login',isAdmin:true});
window.signatureFixture=value=>Object.assign(state,value);
createApp({render:()=>h(Page,state)}).component('UiTransition',UiTransition).mount('#app');</script>`;
const server = dist ? await preview({ root, configFile: false, logLevel: 'error', preview: { host: '127.0.0.1', port: 0 } }) : await createServer({ root, configFile: false, cacheDir: path.join(output, 'vite-cache'), logLevel: 'error',
  plugins: [vue(), { name: 'signature-performance-fixture', configureServer(dev) {
    dev.middlewares.use('/__signature-performance', async (req, res) => {
      res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.end(await dev.transformIndexHtml('/__signature-performance', html));
    });
  } }], server: { host: '127.0.0.1', port: 0, hmr: false } });
if (!dist) await server.listen();
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
const browser = await chromium.launch({ headless: true });
const reads = [], errors = [], gates = [];
let identity = 'A', loggedIn = true, failure = 0, holdNext = false;
const layouts = layoutApiFixture();
const row = (id, n) => ({ person_key: `external:${id}${n}`, record_id: id + n, name: id + ' person ' + n,
  source: 'external', building: 'A楼', signature_status: 'unsigned', has_signature: false,
  can_request: true, can_preview: false, can_receive_message: false });
try {
  const context = await browser.newContext({ viewport: { width, height: width === 1440 ? 900 : width === 402 ? 874 : 956 }, reducedMotion: 'reduce' });
  await context.route('**/*', async route => {
    const request = route.request(), url = new URL(request.url());
    if (url.origin !== origin) return route.abort();
    if (!url.pathname.startsWith('/api/')) return route.continue();
    if (dist && await layouts.handle(route, identity)) return;
    if (dist && url.pathname === '/api/auth/status') return route.fulfill({ json: { ok: true, data: {
      logged_in: loggedIn, user: { open_id: identity, name: 'Fixture ' + identity, role: 'admin', level: 'L0', modules: ['signatures'] },
      scope_options: [{ value: 'A', label: 'A楼' }], login_url: '/login',
    } } });
    if (dist && url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'signature-dist-fixture' } });
    if (dist && !url.pathname.startsWith('/api/signatures/management/')) {
      assert.equal(request.method(), 'GET'); return route.fulfill({ json: { ok: true, data: {} } });
    }
    assert.equal(request.method(), 'GET', 'no writes or sends'); reads.push(url.pathname + url.search);
    if (url.pathname.endsWith('/recipients')) return route.fulfill({ json: { ok: true, data: {
      people: Array.from({ length: 602 }, (_, n) => ({ record_id: 'relay' + n, name: 'Relay ' + n, building: 'B楼' })) } } });
    assert.equal(url.pathname, '/api/signatures/management/people');
    const actor = identity, page = Number(url.searchParams.get('page') || 1), status = failure;
    if (holdNext) { holdNext = false; await new Promise(resolve => gates.push(resolve)); }
    await route.fulfill({ status: status || 200, json: status ? { ok: false, error: 'Fixture read rejected' } : { ok: true, data: {
      people: Array.from({ length: 50 }, (_, n) => row(actor, n + (page - 1) * 50)), count: 1200,
      counts: { unsigned: 1200 }, sources: { staff: { ok: true }, external: { ok: true } },
      recipients: [{ value: 'A' }], scope_options: [{ value: 'A' }], can_manage_directory: true,
      can_create_temporary: true, create_scope_options: [{ value: 'A', label: 'A楼' }],
    } } }).catch(() => undefined);
  });
  const page = await context.newPage(); page.on('pageerror', error => errors.push(error.message));
  page.setDefaultTimeout(12000);
  await page.goto(origin + (dist ? workspace ? '/assets/workspace-v2/index.html?__page=/signature-management' : '/signature-management' : '/__signature-performance'));
  const first = () => page.locator('.people .person-info strong').first();
  await first().filter({ hasText: 'A person 0' }).waitFor();
  assert.equal(reads.length, 1, 'one initial list request');
  await page.getByRole('button', { name: '下一页', exact: true }).click();
  await first().filter({ hasText: 'A person 50' }).waitFor();
  const beforeSearch = reads.length;
  const search = page.locator('.signature-management input[type=search]').first();
  await search.fill('person'); await search.press('Enter');
  await first().filter({ hasText: 'A person 0' }).waitFor(); await page.waitForTimeout(400);
  assert.equal(reads.length, beforeSearch + 1, 'filter reset and Enter do not issue duplicate loads');
  holdNext = true;
  await page.getByRole('button', { name: '刷新签名', exact: true }).click();
  await page.waitForFunction(() => document.querySelector('.directory-panel')?.getAttribute('aria-busy') === 'true');
  identity = 'B';
  if (dist) await page.evaluate(() => window.dispatchEvent(new Event('auth-account-changed')));
  else await page.evaluate(() => window.signatureFixture({ userId: 'B' }));
  try { await first().filter({ hasText: 'B person 0' }).waitFor(); }
  catch (error) { await page.screenshot({ path: path.join(output, 'switch-failure.png'), fullPage: true }); throw error; }
  gates.splice(0).forEach(resolve => resolve()); await page.waitForTimeout(100);
  assert.equal(await page.getByText('A person 0', { exact: true }).count(), 0, 'late old-account data never renders');
  await page.screenshot({ path: path.join(output, 'directory.png') });
  failure = 503; await page.getByRole('button', { name: '刷新签名', exact: true }).click();
  await page.getByText('Fixture read rejected', { exact: true }).waitFor();
  assert.equal(await first().innerText(), 'B person 0', 'safe same-account metadata survives failed refresh');
  failure = 403; await page.getByRole('button', { name: '刷新签名', exact: true }).click();
  await page.locator('.people').waitFor({ state: 'detached' });
  failure = 0; await page.getByRole('button', { name: '刷新签名', exact: true }).click();
  await first().waitFor();
  await page.getByRole('button', { name: '发送签名链接', exact: true }).first().click();
  await page.getByRole('button', { name: /Relay 601/ }).waitFor();
  assert.equal(reads.filter(url => url.startsWith('/api/signatures/management/recipients')).length, 1);
  const beforeRelaySearch = reads.length;
  await page.getByPlaceholder('搜索姓名、工号、岗位或楼栋').fill('Relay 601');
  await page.getByRole('button', { name: /Relay 601/ }).click();
  assert.equal(reads.length, beforeRelaySearch, 'all-recipient search stays local');
  assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), 'viewport must not overflow');
  await page.screenshot({ path: path.join(output, 'recipients.png') });
  identity = 'C';
  if (dist) await page.evaluate(() => window.dispatchEvent(new Event('auth-account-changed')));
  else await page.evaluate(() => window.signatureFixture({ userId: 'C' }));
  await first().filter({ hasText: 'C person 0' }).waitFor();
  assert.equal(await page.locator('dialog[open]').count(), 0, 'account switch closes the old signing dialog');
  assert.equal(await page.getByText('B person 0', { exact: true }).count(), 0);
  assert.equal(await page.locator('dialog .staff-results > button').count(), 0, 'old recipient selection is discarded');
  if (dist) { loggedIn = false; await page.evaluate(() => window.dispatchEvent(new Event('auth-account-changed'))); await page.locator('.signature-management').waitFor({ state: 'detached' }); }
  else { await page.evaluate(() => window.signatureFixture({ loggedIn: false, userId: '' })); await page.getByText('登录后管理人员签名', { exact: true }).waitFor(); }
  assert.equal(await page.locator('dialog[open]').count(), 0);
  assert.equal(await page.getByText('C person 0', { exact: true }).count(), 0);
  assert.equal(reads.filter(url => url.includes('/preview')).length, 0, 'metadata reads never prefetch ink');
  assert.deepEqual(errors, []);
  console.log(`PASS ${dist ? workspace ? 'dist V2' : 'dist legacy' : 'source'} ${width}: initial/filter request counts, late response/account switch, 503 retains safe metadata, 403 clears, 602 recipients and local search, logout, no signature prefetch or writes.`);
  await context.close();
} finally { gates.splice(0).forEach(resolve => resolve()); await browser.close(); if (dist) await new Promise(resolve => server.httpServer.close(resolve)); else await server.close(); }
