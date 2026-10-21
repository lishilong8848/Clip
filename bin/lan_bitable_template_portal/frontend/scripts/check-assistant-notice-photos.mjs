import assert from 'node:assert/strict';
import {mkdir, writeFile} from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {preview} from 'vite';
import {chromium} from 'playwright';
import {coreFixture} from './responsive-audit-core-fixtures.mjs';

const root = fileURLToPath(new URL('..', import.meta.url));
const output = path.resolve(root, '../../../output/playwright/assistant-notice-photos');
await mkdir(output, {recursive: true});
const server = await preview({root, configFile: false, build: {outDir: 'dist'}, logLevel: 'error', preview: {host: '127.0.0.1', port: 0}});
const origin = 'http://127.0.0.1:' + server.httpServer.address().port, browser = await chromium.launch({headless: true});
const results = [];
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jh1cAAAAASUVORK5CYII=', 'base64');
const fields = [
  {name: 'progress', label: '通告进度', type: 'textarea', value: '现场原填写'},
  {name: 'extra_images', path: 'extra_images', label: '现场照片', type: 'file', native_notice_photos: true, maxItems: 20, accept: 'image/*', value: []},
  {name: 'ali_confirmation_images', path: 'ali_confirmation_images', label: '阿里确认截图', type: 'file', native_notice_photos: true, maxItems: 1, accept: 'image/*', value: []},
];
const turns = ['noticeA', 'noticeB'].map((id, i) => ({operation_id: id, question: '核对' + id, answer: '', status: 'completed', phase: 'completed',
  plan: {id, title: (i ? 'B' : 'A') + '楼通告', status: 'needs_input', version: 1, fields: structuredClone(fields), operations: []}}));
const run = {id: 'photo-run', state: 'edit', revision: 1, date: '2026-10-21', slot: 'morning', scope: 'A', items: ['rowA', 'rowB'].map(key => ({
  key, title: '侧栏通告' + key, work_type: 'change', action: 'start', status: '未开始', selected: false,
  fields: [{key: 'progress', label: '进度', kind: 'multiline'}, {key: 'extra_images', label: '现场照片', kind: 'photos', maxItems: 20}, {key: 'ali_confirmation_images', label: '阿里确认截图', kind: 'photos', maxItems: 1}],
  draft: {progress: '', extra_images: [], ali_confirmation_images: []},
}))};

async function check(version, viewport) {
  const context = await browser.newContext({viewport, reducedMotion: 'reduce', permissions: ['clipboard-read', 'clipboard-write']});
  const uploads = [], unexpected = [], errors = [], name = `${version}-${viewport.width}`;
  let failNext = false, hold, release, chooserCount = 0;
  await context.route('**/*', async route => {
    const req = route.request(), url = new URL(req.url()), p = url.pathname;
    if (url.origin !== origin) return route.abort();
    if (!p.startsWith('/api/')) return route.continue();
    const ok = data => route.fulfill({json: {ok: true, data}});
    if (p === '/api/assistant/conversation') return ok({conversation_id: 'notice-photo-isolated', turns, configured: true, enabled: true, busy: false, query_scopes: ['A', 'B'], model_name: '隔离模型', model_options: [], can_manage_settings: false, sync_stamp: '1'});
    if (p === '/api/assistant/notice-panel') return ok({scopes: [{value: 'A', label: 'A楼'}], edition: {id: 'edition-fixture', date: run.date, slot: run.slot, label: '隔离待办'}});
    if (p === '/api/assistant/notice-panel/open') return ok({run});
    if (p === '/api/document-library/tables') return ok({items: [], can_configure: false});
    if (p === '/api/assistant/files' && req.method() === 'POST' || p === '/api/notice-attachments') {
      let fileName;
      if (p === '/api/assistant/files') {
        assert.equal(url.searchParams.get('purpose'), 'notice_photo', 'photo area must not attach to the model composer');
        const form = await new Request('http://localhost/fixture', {method: 'POST', headers: req.headers(), body: req.postDataBuffer()}).formData();
        fileName = form.get('files').name;
      } else fileName = url.searchParams.get('file_name');
      const id = (uploads.length + 1).toString(16).padStart(32, '0');
      uploads.push({id, fileName, path: p, purpose: url.searchParams.get('purpose'), kind: url.searchParams.get('kind'), identity: url.searchParams.get('identity')});
      if (hold) await hold;
      if (failNext) {failNext = false; return route.fulfill({status: 400, json: {ok: false, error: '隔离上传失败'}});}
      return ok(p === '/api/assistant/files'
        ? {files: [{id, name: fileName, size: 32, mime: 'image/png', is_image: true, url: '/api/assistant/files/' + id}]}
        : {upload_id: id, file_name: fileName, size: 32, preview_url: '/api/notice-images/' + 'a'.repeat(32)});
    }
    if (p.startsWith('/api/assistant/files/') || p.startsWith('/api/notice-images/')) return route.fulfill({body: png, contentType: 'image/png'});
    if (req.method() !== 'GET' && !p.startsWith('/api/workspace-layouts/')) {unexpected.push(p); return route.abort();}
    return route.fulfill(await coreFixture({url, method: req.method(), body: req.postData()}) || {json: {ok: true, data: {items: [], records: [], available: false}}});
  });
  const page = await context.newPage(); page.setDefaultTimeout(10000); page.on('pageerror', error => errors.push(error.message));
  page.on('filechooser', () => chooserCount++);
  const plan = id => page.locator(`[data-plan-id="${id}"]`);
  const area = (id, label) => plan(id).getByRole('group', {name: label + '上传区域', exact: true});
  async function transfer(target, kind, fileName, duplicate = false, mime = 'image/png', bytes = 32) {
    await target.evaluate((el, {kind, fileName, duplicate, mime, bytes}) => {
      const data = new DataTransfer(); data.items.add(new File([new Uint8Array(bytes)], fileName, {type: mime}));
      const event = kind === 'paste' ? new ClipboardEvent('paste', {clipboardData: data, bubbles: true, cancelable: true}) : new DragEvent('drop', {dataTransfer: data, bubbles: true, cancelable: true});
      el.dispatchEvent(event); if (duplicate) {el.dispatchEvent(event); window.photoDuplicateEvent = event;}
    }, {kind, fileName, duplicate, mime, bytes});
  }
  async function waitFile(target, fileName) {await target.locator('.plan-file').filter({hasText: fileName}).waitFor();}
  try {
    await page.goto(origin + (version === 'v2' ? '/assets/workspace-v2/index.html?__page=/document-library' : '/document-library'));
    await page.getByRole('button', {name: '打开灯塔助手', exact: true}).click();
    const site = area('noticeA', '现场照片'), ali = area('noticeA', '阿里确认截图');
    await site.waitFor(); await ali.waitFor();
    await plan('noticeA').getByRole('textbox', {name: '通告进度', exact: true}).fill('用户手填进度保留');
    for (const target of [site, ali]) {
      await target.scrollIntoViewIfNeeded(); const box = await target.boundingBox();
      await target.click({position: {x: box.width - 3, y: box.height - 3}});
      assert(await target.evaluate(el => document.activeElement === el)); assert.equal(chooserCount, 0);
      await page.evaluate(async () => {
        const canvas = document.createElement('canvas'); canvas.width = 3; canvas.height = 3;
        canvas.getContext('2d').fillRect(0, 0, 3, 3); const blob = await new Promise(resolve => canvas.toBlob(resolve));
        await navigator.clipboard.write([new ClipboardItem({'image/png': blob})]);
      });
      const before = uploads.length;
      await page.keyboard.press('Control+V'); await target.locator('.plan-file').waitFor();
      assert.equal(uploads.length, before + 1, 'Ctrl+V uses exactly one existing upload request');
    }
    assert.equal(await site.locator('.plan-file').count(), 1); assert.equal(await ali.locator('.plan-file').count(), 1);
    await transfer(site, 'drop', 'site-drop.png', true); await waitFile(site, 'site-drop.png');
    const duplicateCount = uploads.length;
    await site.evaluate(el => el.dispatchEvent(window.photoDuplicateEvent));
    await page.waitForTimeout(80); assert.equal(uploads.length, duplicateCount, 'same DOM event cannot be applied twice after upload completion');
    await transfer(ali, 'drop', 'ali-drop.png'); await waitFile(ali, 'ali-drop.png');
    assert.equal(await ali.locator('.plan-file').count(), 1, 'single-file replacement behavior remains intact');
    assert.equal(await site.locator('.plan-file').filter({hasText: 'ali-drop.png'}).count(), 0);
    const noUpload = uploads.length;
    const normal = await site.evaluate(el => {const data = new DataTransfer(); data.setData('text/plain', 'normal text'); const event = new ClipboardEvent('paste', {clipboardData: data, bubbles: true, cancelable: true}); el.dispatchEvent(event); return event.defaultPrevented;});
    assert.equal(normal, false, 'text paste remains normal');
    await transfer(site, 'paste', 'ignored.txt', false, 'text/plain');
    await transfer(ali, 'drop', 'ignored.pdf', false, 'application/pdf');
    assert.equal(uploads.length, noUpload); assert.equal(chooserCount, 0);
    if (viewport.width === 1440) {
      await transfer(site, 'paste', 'oversize.png', false, 'image/png', 9 * 1024 * 1024);
      await plan('noticeA').getByRole('alert').filter({hasText: '8MiB'}).waitFor(); assert.equal(uploads.length, noUpload);
    }
    failNext = true; await transfer(site, 'drop', 'retry-site.png');
    await plan('noticeA').getByRole('alert').filter({hasText: '隔离上传失败'}).waitFor();
    const failed = uploads.length; assert.equal(await site.locator('.plan-file').count(), 2);
    await transfer(site, 'drop', 'retry-site.png'); await waitFile(site, 'retry-site.png');
    assert.equal(uploads.length, failed + 1); assert.equal(await site.locator('.plan-file').count(), 3);
    const other = area('noticeB', '现场照片');
    await transfer(other, 'paste', 'notice-b.png'); await waitFile(other, 'notice-b.png');
    assert.equal(await site.locator('.plan-file').filter({hasText: 'notice-b.png'}).count(), 0);
    hold = new Promise(resolve => {release = resolve;});
    await transfer(ali, 'drop', 'held-ali.png');
    await ali.getByRole('status').waitFor(); const busyCount = uploads.length;
    await transfer(site, 'paste', 'busy-ignored.png'); assert.equal(uploads.length, busyCount);
    release(); hold = undefined; await waitFile(ali, 'held-ali.png');
    await page.getByRole('button', {name: '收起助手', exact: true}).click();
    await page.getByRole('button', {name: '打开灯塔助手', exact: true}).click(); await waitFile(site, 'retry-site.png'); await waitFile(ali, 'held-ali.png');
    assert.equal(await plan('noticeA').getByRole('textbox', {name: '通告进度', exact: true}).inputValue(), '用户手填进度保留');
    assert.equal(await other.locator('.plan-file').count(), 1);
    await site.scrollIntoViewIfNeeded(); assert(!await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1));
    await page.screenshot({path: path.join(output, name + '-conversation.png'), animations: 'disabled'});
    await ali.scrollIntoViewIfNeeded();
    await page.screenshot({path: path.join(output, name + '-ali.png'), animations: 'disabled'});
    if (viewport.width === 1440) {
      const toggle = page.getByRole('button', {name: '侧边栏（通告待办 / 知识库）', exact: true});
      if (await toggle.getAttribute('aria-expanded') !== 'true') await toggle.click();
      const panel = page.getByRole('region', {name: '通告待办', exact: true});
      if (version === 'legacy') await panel.locator('select[aria-label="楼栋"]').selectOption('A');
      else {await panel.getByRole('combobox', {name: '楼栋', exact: true}).click(); await page.locator('.n-base-select-option').filter({hasText: 'A楼'}).click();}
      await panel.getByRole('checkbox', {name: '选择侧栏通告rowA', exact: true}).check();
      const row = panel.locator('.notice-row').filter({hasText: '侧栏通告rowA'});
      for (const [label, kind] of [['现场照片', 'site'], ['阿里确认截图', 'ali']]) {
        const target = row.getByRole('group', {name: label + '上传区域', exact: true});
        await transfer(target, 'drop', 'sidebar-' + kind + '.png', true); await target.locator('.notice-photo').waitFor();
        assert.equal(uploads.at(-1).kind, kind); assert.equal(uploads.at(-1).identity, 'notice-panel:photo-run:rowA');
      }
      assert.equal(await panel.locator('.notice-row').filter({hasText: '侧栏通告rowB'}).locator('.notice-photo').count(), 0);
      await page.screenshot({path: path.join(output, name + '-sidebar.png'), animations: 'disabled'});
    }
    assert.deepEqual(errors, []); assert.deepEqual(unexpected, []);
    results.push({name, status: 'PASS', uploads: uploads.length}); console.log('PASS assistant notice photos ' + name);
  } catch (error) {
    results.push({name, status: 'FAIL', error: error.stack, errors, unexpected});
    await page.screenshot({path: path.join(output, name + '-failure.png')}); throw error;
  } finally {release?.(); await context.close();}
}
try {
  for (const version of ['legacy', 'v2']) for (const viewport of [{width: 1440, height: 900}, {width: 402, height: 874}, {width: 440, height: 956}]) await check(version, viewport);
} finally {
  await writeFile(path.join(output, 'results.json'), JSON.stringify(results, null, 2));
  await browser.close(); await new Promise(resolve => server.httpServer.close(resolve));
}
