import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/signature-management');
await mkdir(output, { recursive: true });
const server = await preview({ root, logLevel: 'error', preview: { host: '127.0.0.1', port: 0 } });
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
page.setDefaultTimeout(12000);
const errors = [], requests = [], previewRequests = [], apiPosts = [];
let role = 'admin';
let previewFailNext = false;

const PNG_SIGNATURE = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAMgAAABQCAYAAABcbTqwAAAAxUlEQVR4nO3bMQrAIAAEwfz/0wYsU2xKo5mB6wXd0usCAAAAAAAAAAAAAAB4NVYfAL5MIBAEAkEgEARyuGG2cBzOJUMQCASBQBAIBIFAEAgEgUAQCASBQBAIBIFAEAgEgUAQCASBQBAIBIFAEAgEgTCt/rtsew0mjwGCQCAIBIJAIAgEgkAgCASCQCAIBIJAIAgEgkAgCASCQCAIBIJAIAgEgkAgCOSHVv9nNnuOTbk8CAKBIBAIAgEAAAAAAAAAAAAAAMoNzMn+EJC9rUEAAAAASUVORK5CYII=',
  'base64',
);

const people = [
  { person_key: 'staff:rec_staff_1', record_id: 'rec_staff_1', name: '张三', employee_no: 'A001', building: 'A楼',
    source: 'staff', position: '工程师', account_nature: '正式', can_receive_message: true,
    has_signature: true, signature_status: 'signed', signature_reason: '', effective_has_signature: true,
    effective_source: 'staff' },
  { person_key: 'external:rec_ext_1', record_id: 'rec_ext_1', name: '李四', employee_no: 'B002', building: 'B楼',
    source: 'external', position: '临时人员', official_name: '李四',
    has_signature: true, signature_status: 'signed', signature_reason: '', effective_has_signature: true,
    effective_source: 'external' },
  { person_key: 'staff:rec_staff_2', record_id: 'rec_staff_2', name: '王五', employee_no: 'A003', building: 'A楼',
    source: 'staff', position: '工程师', account_nature: '正式', can_receive_message: true,
    has_signature: false, signature_status: 'unsigned', signature_reason: '', effective_has_signature: false,
    effective_source: 'staff' },
];
const duplicateGroups = [{
  name: '张三',
  people: [
    { record_id: 'rec_dup_1', name: '张三', employee_no: 'D001', building: 'D楼', source: 'external', has_signature: true },
    { record_id: 'rec_dup_2', name: '张三', employee_no: 'D002', building: 'D楼', source: 'external', has_signature: true },
  ],
}];

page.on('pageerror', error => errors.push(error.message));
await page.route('**/api/**', async route => {
  const req = route.request(), url = new URL(req.url());
  const pathname = url.pathname;
  requests.push([req.method(), pathname, url.search]);
  if (pathname === '/api/signatures/management/preview') {
    previewRequests.push([req.method(), pathname, url.search]);
    if (previewFailNext) {
      previewFailNext = false;
      return route.fulfill({ status: 500, contentType: 'application/json', body: JSON.stringify({ ok: false, error: '签名图片读取失败，请重试。' }) });
    }
    assert.equal(req.method(), 'GET');
    assert.ok(url.searchParams.get('record_id'), 'preview must carry record_id');
    assert.ok(['staff', 'external'].includes(url.searchParams.get('source')), 'preview source must be staff or external');
    assert.ok(url.searchParams.get('_rt'), 'preview must cache-bust on each open/retry');
    return route.fulfill({ status: 200, contentType: 'image/png', body: PNG_SIGNATURE });
  }
  if (req.method() === 'POST') apiPosts.push([req.method(), pathname, req.postDataJSON?.() || null]);
  const ok = data => route.fulfill({ json: { ok: true, data } });
  if (pathname === '/api/auth/status') return ok({
    logged_in: true, user: { open_id: 'signature-fixture', role, name: role === 'admin' ? '管理员' : '普通用户' },
    scope_options: [{ value: 'A', label: 'A楼' }], login_url: '/api/auth/login',
  });
  if (pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-signature' } });
  if (pathname === '/api/signatures/management/people') return ok({ people, sources: { staff: { ok: true, loaded_at: 1791510000 }, external: { ok: true, loaded_at: 1791510000 } }, counts: { signed: 2, unsigned: 1, resign: 0 }, count: people.length });
  if (pathname === '/api/signatures/management/duplicates') return ok({ groups: duplicateGroups });
  return ok({});
});

async function openPreviewAndCheckLoaded(expectedPreviewCount) {
  await page.getByRole('button', { name: '查看签名', exact: true }).first().click();
  const dlg = page.getByRole('dialog', { name: /签名预览/ }).first();
  await dlg.waitFor();
  await page.waitForFunction(() => {
    const img = document.querySelector('.preview-area img');
    return img && !img.classList.contains('hidden') && img.naturalWidth > 0;
  });
  assert.equal(previewRequests.length, expectedPreviewCount, 'exactly one signature loaded per open attempt (no bulk previews)');
  const img = page.locator('.preview-area img');
  assert.ok((await img.evaluate(el => el.naturalWidth)) > 0, 'visible image must have decoded naturalWidth');
  return dlg;
}

try {
  // ---- Admin flow ----
  await page.goto(origin + '/signature-management');
  await page.getByText('张三', { exact: true }).first().waitFor();
  assert.equal(previewRequests.length, 0, 'no signature preview requests during initial list load');
  assert.equal(await page.getByRole('button', { name: '查看签名', exact: true }).count(), 2, 'admin sees preview for each signed person');
  const unsignedCard = page.locator('.person-card').filter({ hasText: '王五' }).first();
  assert.equal(await unsignedCard.getByRole('button', { name: '查看签名', exact: true }).count(), 0, 'unsigned row exposes no preview control');
  assert.equal(await unsignedCard.locator('img').count(), 0, 'unsigned row renders no signature image');

  // First successful preview from a normal person row.
  let dlg = await openPreviewAndCheckLoaded(1);
  assert.match((await dlg.getAttribute('aria-label')) || '', /张三/, 'preview dialog aria-label carries person name');
  const previewImg = page.locator('.preview-area img');
  assert.equal(await previewImg.evaluate(el => el.naturalWidth), 200, 'synthetic preview naturalWidth must be 200px (not 1px)');
  assert.equal(await previewImg.evaluate(el => el.naturalHeight), 80, 'synthetic preview naturalHeight must be 80px (not 1px)');
  await page.screenshot({ path: path.join(output, 'admin-preview-desktop.png'), fullPage: true });

  // Accidental Enter/submit on the preview form must not send a signing link.
  await dlg.evaluate(() => {
    const form = document.querySelector('dialog form');
    form.dispatchEvent(new Event('submit', { cancelable: true, bubbles: true }));
  });
  await page.waitForTimeout(120);
  assert.equal(apiPosts.length, 0, 'dispatching submit on the preview form must not issue any business POST');

  await dlg.getByRole('button', { name: '关闭' }).first().click();
  assert.equal(await page.locator('.preview-area img').count(), 0, 'close removes preview bytes from DOM');

  // Re-open preview and switch to send form without POSTing.
  dlg = await openPreviewAndCheckLoaded(2);
  await dlg.getByRole('button', { name: '重新签名', exact: true }).click();
  const sendDlg = page.getByRole('dialog', { name: '发送签名链接', exact: true });
  await sendDlg.waitFor();
  assert.equal(apiPosts.length, 0, 're-sign opens the send form without any business POST');
  assert.equal(await sendDlg.count(), 1, 'send form dialog has correct aria label');
  assert.ok(await sendDlg.getByText(/张三/).count() > 0, 're-sign opens send form for the correct signed person (张三)');
  assert.equal(await sendDlg.locator('.preview-area img').count(), 0, 're-sign send form has no leftover preview image');
  await sendDlg.getByRole('button', { name: '取消', exact: true }).click();

  // Retry after a failed read.
  previewFailNext = true;
  await page.getByRole('button', { name: '查看签名', exact: true }).first().click();
  dlg = page.getByRole('dialog', { name: /签名预览/ }).first();
  await dlg.waitFor();
  await page.getByText('签名图片读取失败，请重试。', { exact: true }).waitFor();
  assert.equal(previewRequests.length, 3, 'failed attempt issues one preview request');
  await page.screenshot({ path: path.join(output, 'admin-preview-error.png'), fullPage: true });
  await dlg.getByRole('button', { name: '重试', exact: true }).click();
  await page.waitForFunction(() => {
    const img = document.querySelector('.preview-area img');
    return img && !img.classList.contains('hidden') && img.naturalWidth > 0;
  });
  assert.equal(previewRequests.length, 4, 'retry rerequests with a fresh cache-bust');
  const failedRt = new URLSearchParams(previewRequests[2][2]).get('_rt');
  const retryRt = new URLSearchParams(previewRequests[3][2]).get('_rt');
  assert.ok(failedRt && retryRt && failedRt !== retryRt, 'open and retry cache-busters must differ');
  await page.screenshot({ path: path.join(output, 'admin-preview-retry-desktop.png'), fullPage: true });
  await dlg.getByRole('button', { name: '关闭' }).first().click();

  // Duplicate signed rows: preview must not toggle the merge checkbox.
  await page.getByRole('button', { name: '重复人员核对', exact: true }).click();
  await page.getByText('重复人员预检', { exact: true }).waitFor();
  const dupRow = page.locator('.duplicate-row').filter({ hasText: '张三' }).first();
  const checkbox = dupRow.locator('input[type=checkbox]').first();
  const before = await checkbox.isChecked();
  await dupRow.getByRole('button', { name: '查看签名', exact: true }).click();
  dlg = page.getByRole('dialog', { name: /签名预览/ }).first();
  await dlg.waitFor();
  await page.waitForFunction(() => {
    const img = document.querySelector('.preview-area img');
    return img && !img.classList.contains('hidden') && img.naturalWidth > 0;
  });
  assert.equal(await checkbox.isChecked(), before, 'preview from duplicate row must not toggle merge checkbox');
  assert.equal(previewRequests.length, 5);
  await dlg.getByRole('button', { name: '关闭' }).first().click();

  // Narrow desktop screenshot of a live preview.
  await page.setViewportSize({ width: 1280, height: 800 });
  await page.getByRole('button', { name: '查看签名', exact: true }).first().click();
  dlg = page.getByRole('dialog', { name: /签名预览/ }).first();
  await dlg.waitFor();
  await page.waitForFunction(() => {
    const img = document.querySelector('.preview-area img');
    return img && !img.classList.contains('hidden') && img.naturalWidth > 0;
  });
  assert.equal(await page.locator('.preview-area img').evaluate(el => el.naturalWidth), 200, 'narrow preview retains synthetic width 200');
  assert.equal(await page.locator('.preview-area img').evaluate(el => el.naturalHeight), 80, 'narrow preview retains synthetic height 80');
  await page.screenshot({ path: path.join(output, 'admin-preview-narrow.png'), fullPage: true });
  await dlg.getByRole('button', { name: '关闭' }).first().click();

  assert.deepEqual(errors, []);

  // ---- Normal user flow (fresh navigation resets app auth state) ----
  const previewCountBeforeUserNav = previewRequests.length;
  role = 'user';
  await page.goto(origin + '/signature-management');
  await page.getByText('张三', { exact: true }).first().waitFor();
  assert.equal(await page.getByRole('button', { name: '查看签名', exact: true }).count(), 0, 'normal user gets no preview controls');
  assert.equal(previewRequests.length, previewCountBeforeUserNav, 'normal user triggers no additional signature image requests');
  assert.equal(await page.locator('.preview-area img').count(), 0, 'normal user renders no preview image');
  await page.screenshot({ path: path.join(output, 'normal-user-no-preview.png'), fullPage: true });

  console.log(JSON.stringify({ ok: true, screenshots: output, checks: 'admin no initial preview requests, unsigned rows expose no preview controls/images, synthetic 200x80 preview with visible naturalWidth and aspect, modal close clears DOM, submit on preview form does not POST, re-sign opens send form for correct person without POST, retry on failed read with distinct cache-busters, duplicate-row preview without merge toggle, normal user no preview controls/images and no extra requests, full + narrow(1280x800) desktop screenshots' }));
} catch (error) {
  await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true });
  throw error;
} finally {
  await browser.close();
  await new Promise(resolve => server.httpServer.close(resolve));
}