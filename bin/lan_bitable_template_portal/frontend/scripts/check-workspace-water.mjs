import assert from 'node:assert/strict';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { build as bundle } from 'esbuild';
import { chromium } from 'playwright';
import { build, preview } from 'vite';
import vue from '@vitejs/plugin-vue';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/workspace-water');
await mkdir(output, { recursive: true });
const helper = await bundle({ entryPoints: [path.join(root, 'src/workspace-v2/water.ts')], bundle: true, write: false, format: 'esm' });
const water = await import('data:text/javascript;base64,' + Buffer.from(helper.outputFiles[0].text).toString('base64'));
assert.equal(water.waterNumber(null), null);
assert.equal(water.waterNumber(''), null);
assert.equal(water.waterNumberText(undefined), '—');
assert.equal(water.waterNumberText(0), '0');
assert.equal(water.validWaterDate('2026-02-30'), false);
const filters = { range: '自定义日期', startDate: '2026-10-01', endDate: '', meter: '', frequency: '', shift: '', query: '' };
assert.throws(() => water.waterQuery('A', filters), /完整/);
assert.throws(() => water.waterQuery('A', { ...filters, endDate: '2026-09-01' }), /晚于/);
const rollover = water.waterQuery('A', { ...filters, range: '近3天' }, 1, '2026-11-01');
assert.equal(rollover.get('start_date'), '2026-10-30'); assert.equal(rollover.get('end_date'), '2026-11-01');
assert.deepEqual(water.waterTrend([
  { frequency: '日', statistic_date: '2026-10-01', corrected_usage: 2, computed_usage: 99 },
  { frequency: '日', statistic_date: '2026-10-01', computed_usage: 3 },
  { frequency: '周', statistic_date: '2026-10-01', computed_usage: 100 },
]).items, [{ label: '2026-10-01', value: 5 }]);
assert.deepEqual(water.waterTrend([{ frequency: '日', statistic_date: '2026-10-01', computed_usage: null }]), { items: [], missing: 1 });
const oldPage = await readFile(path.join(root, 'src/components/WaterManagementPage.vue'), 'utf8');
const site = process.env.CLIPFLOW_WATER_PREVIEW_SITE || path.join(output, 'site');
if (!process.argv.includes('--full-workspace') && !process.argv.includes('--reuse-build')) {
  // Compile the real water components and common UI, independent of concurrent edits to other pages.
  const host = path.join(output, 'host'); await mkdir(host, { recursive: true });
  const sourcePath = path.join(root, 'src').replaceAll('\\', '/');
  await writeFile(path.join(host, 'WaterHost.vue'), `<template><NConfigProvider :theme-overrides="theme" :locale="zhCN" :date-locale="dateZhCN"><NMessageProvider placement="bottom-right" :duration="0" closable><NDialogProvider><div class="v2-shell"><header class="v2-header"><div class="v2-brand"><img src="/assets/vnet-logo.png" alt="世纪互联 VNET"><span>灯塔<small>南通运维工作空间</small></span></div><span>{{ user.name }}</span></header><SidebarNavigation :collapsed="false" :options="menus" selected="water" @select="navigate('/')"/><main class="v2-main"><WaterWorkspace v-if="ready&&route.pathname==='/water-management'" :key="scope" :scope="scope" :scope-options="scopes" @switch-scope="switchScope"/><h1 v-else-if="ready">个人工作台</h1></main></div></NDialogProvider></NMessageProvider></NConfigProvider></template>
<script setup>
import {computed,onMounted,onBeforeUnmount,ref} from 'vue';
import {NConfigProvider,NDialogProvider,NMessageProvider,zhCN,dateZhCN} from 'naive-ui';
import WaterWorkspace from '${sourcePath}/workspace-v2/WaterWorkspace.vue';
import SidebarNavigation from '${sourcePath}/workspace-v2/SidebarNavigation.vue';
import {theme} from '${sourcePath}/workspace-v2/theme';
import {requestJson,setReadCacheIdentity} from '${sourcePath}/workspace-v2/client';
import {navigate} from '${sourcePath}/workspace-v2/navigation';
import {logicalUrl} from '${sourcePath}/workspace-v2/version';
const route=ref(logicalUrl()),scope=computed(()=>route.value.searchParams.get('scope')||'A'),ready=ref(false),user=ref({}),scopes=ref([]),menus=[{label:'个人工作台',key:'home'},{label:'水耗管理',key:'water'}];
function changed(){route.value=logicalUrl();}function switchScope(value){const url=logicalUrl();url.searchParams.set('scope',value);navigate(url);}
onMounted(async()=>{window.addEventListener('popstate',changed);const auth=await requestJson('/api/auth/status');user.value=auth.user;scopes.value=auth.scope_options;setReadCacheIdentity(JSON.stringify(auth));ready.value=true;});
onBeforeUnmount(()=>window.removeEventListener('popstate',changed));
</script>`);
  await writeFile(path.join(host, 'main.ts'), `import {createApp} from 'vue';import Host from './WaterHost.vue';
import '${sourcePath}/global.css';import '${sourcePath}/workspace-v2/workspace.css';import '${sourcePath}/workspace-v2/interactions.css';import '${sourcePath}/workspace-v2/feedback.css';import '${sourcePath}/workspace-v2/feedbackToasts.css';
import {installFeedbackMotion} from '${sourcePath}/workspace-v2/feedbackMotion';import {installActionMotion} from '${sourcePath}/workspace-v2/actionMotion';
document.documentElement.classList.add('workspace-v2');installActionMotion();installFeedbackMotion();createApp(Host).mount('#app');`);
  console.log('Building isolated production water components');
  const result = await build({ root, configFile: false, plugins: [vue({template:{transformAssetUrls:false}})], resolve: { dedupe: ['vue','naive-ui'] }, base: '/assets/workspace-v2/', logLevel: 'error', build: { write: false, cssCodeSplit: false, rollupOptions: { input: path.join(host, 'main.ts'), output: { entryFileNames: 'app-[hash].js', chunkFileNames: 'part-[hash].js', assetFileNames: 'res-[hash][extname]' } } } });
  const files = (Array.isArray(result) ? result[0] : result).output, assets = path.join(site, 'assets/workspace-v2');
  await mkdir(assets, { recursive: true });
  for (const file of files) await writeFile(path.join(assets, file.fileName), file.type === 'chunk' ? file.code : file.source);
  const script = files.find(file => file.type === 'chunk' && file.isEntry).fileName;
  const css = files.filter(file => file.fileName.endsWith('.css')).map(file => `<link rel="stylesheet" href="/assets/workspace-v2/${file.fileName}">`).join('');
  await writeFile(path.join(assets, 'index.html'), `<!doctype html><html lang="zh-CN"><head><meta name="viewport" content="width=device-width,initial-scale=1">${css}</head><body><div id="app"></div><script type="module" src="/assets/workspace-v2/${script}"></script></body></html>`);
}
const server = await preview({ root, configFile: false, build: { outDir: site }, logLevel: 'error', preview: { host: '127.0.0.1', port: 0 } });
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
const entry = '/assets/workspace-v2/index.html';
const browser = await chromium.launch({ headless: true });
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jR1cAAAAASUVORK5CYII=', 'base64');
const day = water.waterDay();
const records = [
  { record_id: 'water-fixture-1', statistic_date: day, meter: 'A楼生活水表', frequency: '日', shift: '白', meter_value: 1200, corrected_usage: null, computed_usage: 25, photo_count: 1 },
  { record_id: 'water-fixture-2', statistic_date: day, meter: 'A楼冷却补水表', frequency: '日', shift: '白', meter_value: 2500, corrected_usage: null, computed_usage: 30, photo_count: 0 },
];
const failures = [], requests = [];
async function fixture(options = {}) {
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const state = { admin: true, failBoot: false, failList: false, failUpload: false, snapshotReady: true, snapshotVersion: 1, refreshing: false, saveError: '', saveGate: null, readGate: null, posts: [], uploads: 0, ...options };
  await ctx.route('**/*', async route => {
    const request = route.request(), url = new URL(request.url());
    if (url.origin !== origin && !['blob:', 'data:'].includes(url.protocol)) return route.abort();
    if (url.pathname === '/assets/connection-guard.js') return route.fulfill({ contentType: 'application/javascript', body: '' });
    if (url.pathname === '/assets/vnet-logo.png') return route.fulfill({ contentType: 'image/png', body: await readFile(path.join(root, 'public/assets/vnet-logo.png')) });
    if (!url.pathname.startsWith('/api/')) return route.continue();
    requests.push({ path: url.pathname, method: request.method(), params: Object.fromEntries(url.searchParams) });
    const ok = data => route.fulfill({ json: { ok: true, data } });
    const fail = (text, status = 503, extras = {}) => route.fulfill({ status, json: { ok: false, error: text, ...extras } });
    if (url.pathname === '/api/auth/status') return ok({ logged_in: true, user: { open_id: 'ou_water_fixture', name: '隔离水耗验收', role: state.admin ? 'admin' : 'member', level: state.admin ? 'L1' : 'L4', category: 'operator', modules: ['water'] }, scope_options: [{ value: 'A', label: 'A楼' }, { value: 'B', label: 'B楼' }] });
    if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'water-isolated' } });
    if (url.pathname === '/api/capacity/water/bootstrap') {
      if (state.failBoot) return fail('基础信息暂不可用');
      return ok({ scope: 'A', month: day.slice(0, 7), summary: { record_count: 2, total_usage: 55, photo_records: 1 }, options: { meters: ['A楼生活水表', 'A楼冷却补水表'], frequencies: ['日', '周', '月'], shifts: ['白', '夜'] }, snapshot: { exists: state.snapshotReady, snapshot_version: state.snapshotVersion, refreshing: state.refreshing, refreshed_at: 1791940000 }, permissions: { is_admin: state.admin, can_create: state.admin } });
    }
    if (url.pathname === '/api/capacity/water/records' && request.method() === 'GET') {
      const gate = state.readGate; if (gate) { state.readGate = null; await gate.promise; }
      if (state.failList) return fail('记录读取暂不可用');
      const data = !state.snapshotReady || url.searchParams.get('scope') === 'B' ? [] : records;
      try { return await ok({ records: data, total: data.length, snapshot: { exists: state.snapshotReady } }); } catch { return; }
    }
    if (url.pathname === '/api/capacity/water/uploads') {
      state.uploads++;
      if (state.failUpload) { state.failUpload = false; return fail('照片上传暂不可用', 400); }
      return ok({ upload_id: 'water-upload-' + state.uploads });
    }
    if (request.method() === 'PATCH' || request.method() === 'POST' && url.pathname === '/api/capacity/water/records') {
      const payload = request.postDataJSON(); state.posts.push(payload);
      if (state.saveGate) await state.saveGate.promise;
      if (state.saveError === 'unknown') { state.saveError = ''; return fail('保存响应暂不可用'); }
      if (state.saveError === 'abnormal' && !payload.large_change_confirmed) return fail('读数变化较大，请补充原因', 409, { error_code: 'confirmation_required', details: { kind: 'water_large_change', previous_date: water.waterDay(-1), old_value: 100, new_value: payload.meter_value, ratio: 1 } });
      return ok({ record_id: 'water-fixture-1', warnings: [] });
    }
    if (url.pathname.startsWith('/api/capacity/water/records/')) {
      const row = records.find(row => url.pathname.endsWith(row.record_id));
      if (state.partialDetail) return ok({ ...row, version: 'original-version' });
      return ok({ ...row, version: 'original-version', previous_date_text: water.waterDay(-1), previous_value_text: '1175', yoy_ratio: 0.05, photos: row.photo_count ? [{ image_id: 'water-photo-1', name: '水表现场.png' }] : [], edit_policy: { is_admin: state.admin, can_edit: state.admin || options.allowEdit, remaining_edits: state.admin ? null : options.allowEdit ? 2 : 0 } });
    }
    if (url.pathname.startsWith('/api/capacity/water/images/')) return route.fulfill({ contentType: 'image/png', body: png });
    if (request.method() !== 'GET') throw new Error('Unexpected fixture write ' + url.pathname);
    return ok({ items: [], records: [], scopes: {}, options: {}, summary: {} });
  });
  const page = await ctx.newPage();
  if (options.clock) await page.clock.install();
  page.setDefaultTimeout(10000);
  page.on('pageerror', error => failures.push(error.message));
  await page.goto(`${origin}${entry}?__page=/water-management&scope=A`);
  return { page, ctx, state };
}
function gate() { let release; const promise = new Promise(resolve => { release = resolve; }); return { promise, release }; }
async function openFirst(page) { await page.getByRole('button', { name: '填写A楼生活水表' + day }).click(); await page.getByLabel('本次水表读数', { exact: true }).waitFor(); }
async function capture(page, name) { await page.screenshot({ path: path.join(output, name + '.png'), fullPage: true }); }
try {
  if (!process.argv.includes('--mobile-only')) {
  console.log('Checking independent read and unknown data states');
  const failed = await fixture({ failBoot: true, failList: true });
  await failed.page.getByText('水耗记录暂不可用', { exact: true }).waitFor();
  assert.equal(await failed.page.getByText('当前条件下暂无水耗记录', { exact: true }).count(), 0);
  assert.match(await failed.page.locator('.water-month-summary').innerText(), /记录\s+—/);
  failed.state.failList = false;
  await failed.page.getByRole('button', { name: '重试读取', exact: true }).click();
  await failed.page.getByRole('button', { name: '填写A楼生活水表' + day }).waitFor();
  assert.equal(await failed.page.locator('.water-read-error').count(), 0, 'successful retry removes old read errors');
  assert.match(await failed.page.locator('.water-month-summary').innerText(), /记录\s+—/, 'failed bootstrap must not block records or turn unknown statistics into zero');
  await failed.ctx.close();

  const incomplete = await fixture({ partialDetail: true });
  await incomplete.page.getByRole('button', { name: '填写A楼生活水表' + day }).click();
  await incomplete.page.getByText('记录读取失败', { exact: true }).waitFor();
  assert.equal(await incomplete.page.getByRole('button', { name: '保存记录', exact: true }).count(), 0, 'incomplete photo metadata cannot overwrite existing attachments');
  incomplete.state.partialDetail = false;
  await incomplete.page.getByRole('button', { name: '重新读取记录', exact: true }).click();
  await incomplete.page.getByLabel('本次水表读数', { exact: true }).waitFor();
  await incomplete.ctx.close();

  const initial = await fixture({ snapshotReady: false, snapshotVersion: undefined, refreshing: true, clock: true });
  await initial.page.getByText('等待首次数据同步', { exact: true }).waitFor();
  assert.match(await initial.page.locator('.water-month-summary').innerText(), /记录\s+—/);
  initial.state.snapshotReady = true; initial.state.snapshotVersion = 1; initial.state.refreshing = false;
  await initial.page.clock.fastForward(3100);
  await initial.page.getByRole('button', { name: '填写A楼生活水表' + day }).waitFor();
  await initial.ctx.close();

  console.log('Checking editing, attachments, idempotent retries and abnormal confirmation');
  const { page, ctx, state } = await fixture();
  await openFirst(page);
  for (const width of [1366, 1440, 1920]) {
    await page.setViewportSize({ width, height: 900 });
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
    await capture(page, 'desktop-editor-' + width);
  }
  const input = page.getByLabel('本次水表读数', { exact: true });
  await input.fill('1300');
  await page.getByRole('button', { name: '返回工作台', exact: true }).click();
  await page.getByRole('button', { name: '继续填写', exact: true }).click();
  assert.equal(await input.inputValue(), '1300');
  assert.match(page.url(), /water-management/);
  state.failUpload = true;
  await page.getByLabel('添加水表照片', { exact: true }).setInputFiles({ name: 'fixture.png', mimeType: 'image/png', buffer: png });
  await page.getByRole('button', { name: '重试此照片', exact: true }).waitFor();
  assert.equal(await page.locator('.water-photo-grid article.failed img').count(), 1);
  await page.getByRole('button', { name: '保存记录', exact: true }).click();
  await page.getByText('请重试失败照片，或移除后重新上传。', { exact: true }).waitFor();
  assert.equal(state.posts.length, 0, 'validation failure must not send a save');
  await page.getByRole('button', { name: '重试此照片', exact: true }).click();
  await page.getByText('待保存', { exact: true }).waitFor();
  assert.equal(state.uploads, 2);
  state.saveError = 'unknown';
  await page.getByRole('button', { name: '保存记录', exact: true }).click();
  await page.getByRole('button', { name: '继续原保存任务', exact: true }).waitFor();
  assert.equal(await page.locator('.water-editor-error').getAttribute('popover'), null, 'record errors stay inline and cannot cover retry');
  assert.equal(await input.isDisabled(), true);
  await page.getByRole('button', { name: '继续原保存任务', exact: true }).click();
  await page.locator('.water-notice').filter({ hasText: '已保存' }).waitFor();
  assert.equal(await page.locator('.water-notice').getAttribute('popover'), null, 'saved notices cannot cover the next save');
  assert.equal(state.posts.length, 2);
  assert.deepEqual(state.posts[1], state.posts[0], 'uncertain retry keeps the complete original payload and operation id');
  assert.equal(state.posts[0].expected_version, 'original-version');
  assert.deepEqual(state.posts[0].retained_image_ids, ['water-photo-1']);
  assert.deepEqual(state.posts[0].upload_ids, ['water-upload-2']);
  await openFirst(page);
  await input.fill('1450');
  state.saveError = 'abnormal';
  await page.getByRole('button', { name: '保存记录', exact: true }).click();
  await page.getByLabel('异常备注', { exact: true }).waitFor();
  const before = state.posts.length;
  await page.getByRole('button', { name: '确认并保存', exact: true }).click();
  assert.equal(state.posts.length, before);
  await page.getByLabel('异常备注', { exact: true }).fill('已现场核验，用水量增加。');
  await page.getByRole('button', { name: '确认并保存', exact: true }).click();
  await page.locator('.water-editor').waitFor({ state: 'detached' });
  assert.equal(state.posts.at(-1).large_change_confirmed, true);
  assert.equal(state.posts.at(-1).abnormal_note, '已现场核验，用水量增加。');
  assert.equal(state.posts.at(-1).operation_id, state.posts.at(-2).operation_id);
  await openFirst(page); await input.fill('1350'); state.saveError = ''; state.saveGate = gate();
  await page.getByRole('button', { name: '保存记录', exact: true }).click();
  await page.getByText('正在保存，请等待结果', { exact: true }).waitFor();
  await page.getByRole('button', { name: '返回工作台', exact: true }).click();
  assert.match(page.url(), /water-management/);
  state.saveGate.release(); state.saveGate = null;
  await page.locator('.water-editor').waitFor({ state: 'detached' });
  await page.getByRole('button', { name: '趋势', exact: true }).click();
  await page.locator('.v2-chart-canvas svg').waitFor();
  await capture(page, 'desktop-list-trend');
  await ctx.close();

  console.log('Checking invalid-date response races');
  const race = await fixture();
  await race.page.getByRole('button', { name: '填写A楼生活水表' + day }).waitFor();
  const delayed = gate(); race.state.readGate = delayed;
  await race.page.getByText('自定义日期', { exact: true }).click();
  await race.page.getByLabel('筛选开始日期', { exact: true }).fill('');
  await race.page.keyboard.press('Tab');
  await race.page.getByText('请选择完整的开始日期和结束日期。', { exact: true }).waitFor();
  delayed.release();
  await race.page.waitForTimeout(350);
  assert.equal(await race.page.locator('.water-stale').count(), 1);
  assert.equal(await race.page.getByRole('button', { name: '填写A楼生活水表' + day }).isDisabled(), true, 'old results cannot become current after an invalid date');
  await race.ctx.close();
  }

  console.log('Checking mobile layout and original edit permissions');
  const readonly = await fixture({ admin: false });
  await openFirst(readonly.page);
  assert.equal(await readonly.page.getByRole('button', { name: '新增录入', exact: true }).count(), 0);
  assert.equal(await readonly.page.getByLabel('本次水表读数', { exact: true }).isDisabled(), true);
  assert.equal(await readonly.page.getByRole('button', { name: '保存记录', exact: true }).isDisabled(), true);
  await readonly.ctx.close();
  const handset = await fixture({ admin: false, allowEdit: true });
  for (const width of [360, 390, 430]) {
    await handset.page.setViewportSize({ width, height: 844 });
    await handset.page.locator('.water-mobile-record').first().waitFor();
    await handset.page.waitForFunction(()=>document.querySelector('.v2-brand img')?.naturalWidth>0);
    assert.equal(await handset.page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
    await capture(handset.page, 'mobile-list-' + width);
    await handset.page.locator('.water-mobile-record').first().click();
    await handset.page.getByLabel('本次水表读数', { exact: true }).waitFor();
    try { await handset.page.waitForFunction(()=>{const box=document.querySelector('.water-editor')?.getBoundingClientRect();return box&&Math.abs(box.left)<1&&Math.abs(box.right-innerWidth)<1;}); }
    catch(error) { console.log(await handset.page.evaluate(()=>({width:innerWidth,editor:document.querySelector('.water-editor')?.getBoundingClientRect().toJSON(),drawers:[...document.querySelectorAll('.n-drawer,.n-drawer-container')].map(el=>({box:el.getBoundingClientRect().toJSON(),style:el.getAttribute('style'),cssWidth:getComputedStyle(el).width}))})));await capture(handset.page,'mobile-framing-error');throw error; }
    assert.equal(await handset.page.getByLabel('本次水表读数', { exact: true }).isEnabled(), true);
    assert.equal(await handset.page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false);
    const box = await handset.page.locator('.water-editor-actions').boundingBox();
    assert(box.y + box.height <= 845, 'mobile save remains inside the viewport');
    await capture(handset.page, 'mobile-editor-' + width);
    await handset.page.getByRole('button', { name: '关闭水耗编辑', exact: true }).click();
    await handset.page.locator('.water-editor').waitFor({ state: 'detached' });
  }
  assert.equal(handset.state.posts.length, 0);
  await handset.ctx.close();
  assert.deepEqual(failures, []);
  assert.equal(await readFile(path.join(root, 'src/components/WaterManagementPage.vue'), 'utf8'), oldPage);
  console.log('PASS: water state helpers, original API payloads, isolated page reads/saves, draft guards, failed-photo retries, abnormal notes, permissions, desktop/mobile screenshots.');
} finally { await browser.close(); await server.close(); }
