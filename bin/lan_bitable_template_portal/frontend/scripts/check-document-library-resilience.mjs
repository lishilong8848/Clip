import assert from 'node:assert/strict';
import { tmpdir } from 'node:os';
import { mkdtemp, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';
import { documentFixture, startDocumentLibraryPreview } from './document-library-preview.mjs';

const server = await startDocumentLibraryPreview();
const browser = await chromium.launch({ headless: true });
const checks = [], failures = [];
const folder = await mkdtemp(path.join(tmpdir(), 'document-library-resilience-'));
console.log('Evidence:', folder);
async function checkVersion(version) {
  Object.assign(server.fixture, documentFixture());
  const fixture = server.fixture;
  fixture.dateTime = true;
  fixture.records[0].fields.fldDate = Date.parse('2026-10-23T08:35:00+08:00');
  fixture.tables[0].summary = { records: 3, pending_items: 1, updated_at: 1792376400, sync_status: 'completed', error: '' };
  const context = await browser.newContext({ serviceWorkers: 'block', viewport: { width: 1440, height: 900 }, timezoneId: 'America/New_York', reducedMotion: 'reduce' });
  const errors = [];
  let allowed = false;
  const entry = version === 'v2' ? '/assets/workspace-v2/index.html?__page=/document-library' : '/document-library?frontend=legacy';
  const step = message => { checks.push(version + ': ' + message); console.log('PASS', checks.at(-1)); };
  const page = await context.newPage();
  page.setDefaultTimeout(10000);
  page.on('pageerror', error => errors.push(error.message));
  try {
    await context.route('**/*', async route => {
      const url = new URL(route.request().url());
      if (url.origin !== server.origin) return route.abort();
      if (url.pathname === '/api/auth/status') {
        const response = await route.fetch();
        const json = await response.json();
        json.data.user.modules = allowed ? ['documents'] : ['knowledge'];
        return route.fulfill({ response, json });
      }
      return route.continue();
    });
    const button = name => page.getByRole('button', { name, exact: true });
    const drawer = () => page.locator('.n-drawer-content').filter({
      has: page.locator(version === 'v2' ? '.sdoc-detail' : '.document-detail'),
    });
    await page.goto(server.origin + entry);
    await page.getByText('没有该功能权限', { exact: true }).waitFor();
    assert.equal(fixture.calls.length, 0, 'knowledge alone must never read document-library data');
    step('knowledge-only access denied before any library request');
    allowed = true;
    await page.reload();
    const tile = page.locator('article').filter({ has: button('打开数据表：电气技术资料') });
    await tile.getByText(/^3 条缓存记录(?:$| ·)/).waitFor();
    assert.equal(await page.locator('html.workspace-v2').count(), Number(version === 'v2'), 'correct frontend is mounted');
    step('documents grant opens ' + entry + ' and displays 3 cached records');
    await button('打开数据表：电气技术资料').click();
    await button('查看资料：A楼高压配电操作规程').click();
    await drawer().getByRole('button', { name: '编辑资料', exact: true }).waitFor();
    assert.match(await drawer().innerText(), /08:35/);
    assert.equal(await drawer().locator('.document-system-fields').getAttribute('open'), null);
    await button('预览附件：南通标识.png').click();
    await page.locator('.document-preview img').waitFor();
    await page.waitForFunction(() => document.querySelector('.document-preview img')?.naturalWidth > 0);
    assert.match(await page.locator('.document-preview img').getAttribute('src'), /preview=1/);
    await page.keyboard.press('Escape');
    await page.locator('.document-preview').waitFor({ state: 'hidden' });
    step('image attachment loads through the preview endpoint and Escape returns to the drawer');
    await drawer().getByRole('button', { name: '编辑资料', exact: true }).click();
    const effectiveDate = drawer().locator('[data-field-id="fldDate"]').getByRole('textbox');
    await effectiveDate.fill('2026-10-24 09:40');
    await effectiveDate.press('Enter');
    await drawer().getByLabel('资料名称', { exact: true }).click();
    await drawer().getByLabel('变更原因', { exact: true }).fill('核对日期时间');
    await drawer().getByRole('button', { name: '保存资料', exact: true }).click();
    await drawer().getByRole('button', { name: '编辑资料', exact: true }).waitFor();
    assert.equal(fixture.writes.at(-1).body.fields.fldDate, Date.parse('2026-10-24T09:40:00+08:00'));
    step('date-time roundtrip uses Shanghai independent of browser timezone');

    await drawer().getByRole('button', { name: '编辑资料', exact: true }).click();
    await drawer().getByLabel('文件页数', { exact: true }).fill('25');
    await drawer().getByLabel('资料名称', { exact: true }).fill('冲突核对后的资料');
    await drawer().getByLabel('变更原因', { exact: true }).fill('处理字段冲突');
    fixture.flags.conflict = true;
    await drawer().getByRole('button', { name: '保存资料', exact: true }).click();
    await drawer().getByRole('button', { name: '读取最新版本，保留输入', exact: true }).click();
    await page.getByRole('region', { name: '字段冲突对比' }).waitFor();
    assert(await drawer().getByRole('button', { name: '保存资料', exact: true }).isDisabled());
    assert(await drawer().getByRole('button', { name: '重新读取资料', exact: true }).isDisabled(), 'Unresolved edits cannot be erased by a second refresh');
    await button('使用最新值').click();
    await drawer().getByRole('button', { name: '保存资料', exact: true }).click();
    await drawer().getByRole('button', { name: '编辑资料', exact: true }).waitFor();
    assert.equal(fixture.records[0].fields.fldCount, 18);
    assert.equal(fixture.records[0].fields.fldTitle, '冲突核对后的资料', 'nonconflicting title edit survives the merge');
    step('field conflicts block save until an explicit choice');

    await drawer().getByRole('button', { name: '编辑资料', exact: true }).click();
    await drawer().getByLabel('资料名称', { exact: true }).fill('刷新后核验的资料');
    await drawer().getByLabel('变更原因', { exact: true }).fill('隔离回执恢复');
    fixture.flags.loseReceipt = true;
    await drawer().getByRole('button', { name: '保存资料', exact: true }).click();
    await drawer().getByText('操作回执尚未确认，输入及原操作编号已保留。请核验后按原操作重试。', { exact: true }).waitFor();
    const applied = fixture.applied;
    const submissions = fixture.writes.length;
    const operationId = fixture.writes.at(-1).body.operation_id;
    page.once('dialog', dialog => dialog.accept());
    await page.reload();
    await button('打开数据表：电气技术资料').click();
    await button('核验原操作').click();
    await page.getByText('原操作已核验成功，没有重复提交。', { exact: true }).waitFor();
    assert.equal(fixture.applied, applied);
    assert.equal(fixture.writes.length, submissions, 'verification cannot resubmit even an idempotent write');
    assert.equal(fixture.calls.filter(call => call.method === 'POST' &&
      call.path === '/tables/electrical/operations/' + operationId + '/verify').length, 1);
    assert.equal(fixture.pendingOperations.length, 0);
    step('recovery survives reload and never repeats a mutation');

    if (version === 'v2') await page.getByText('数据表管理', { exact: true }).click();
    await button('本地附件缓存').click();
    const storage = version === 'v2'
      ? page.getByRole('dialog', { name: '本地附件缓存', exact: true })
      : page.locator('.document-storage');
    await storage.getByText('可安全清理', { exact: true }).waitFor();
    await button('清理无引用缓存').click();
    await storage.filter({ hasText: '0 个' }).waitFor();
    assert.equal(fixture.storage.removed, 1);
    assert.equal(fixture.storage.files, 2);
    assert.equal(fixture.storage.orphan_files, 0);
    step('cleanup removes only the single unreferenced fixture file');
    await page.getByRole('button', { name: '关闭', exact: true }).click();
    await button('查看资料：刷新后核验的资料').click();
    await drawer().getByRole('button', { name: '编辑资料', exact: true }).waitFor();
    // The drawer mounts asynchronously; measure it after its slide-in transition.
    await page.locator('.n-drawer[class*="-enter-"]').waitFor({ state: 'detached' });
    for (const viewport of [{ name: 'MacBookPro', width: 1440, height: 900 }, { name: 'iPhone17Pro', width: 402, height: 874 },
        { name: 'iPhone17ProMax', width: 440, height: 956 }, { name: 'small', width: 360, height: 640 }]) {
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.evaluate(async () => { await Promise.all(document.getAnimations().filter(animation => animation.effect?.getTiming().iterations !== Infinity).map(animation => animation.finished.catch(() => {}))); await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))); });
      const overflow = await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1);
      assert.equal(overflow, false, version + ' global overflow ' + viewport.name);
      const close = await drawer().getByRole('button', { name: '关闭', exact: true }).boundingBox();
      assert(close && close.x >= 0 && close.x + close.width <= viewport.width + 1 && close.y >= 0 && close.y + close.height <= viewport.height + 1,
        `${version} ${viewport.name}: drawer close button must stay in the viewport, got ${JSON.stringify(close)}`);
      await page.screenshot({ path: path.join(folder, version + '-' + viewport.name + '.png'), fullPage: true });
    }
    step('PC and phone controls remain reachable');
    assert.deepEqual(errors, []);
  } catch (error) {
    await page.screenshot({ path: path.join(folder, version + '-failure.png') });
    throw error;
  } finally { await context.close(); }
}

try {
  for (const version of ['legacy', 'v2']) {
    try { await checkVersion(version); }
    catch (error) {
      failures.push(version + ': ' + error.message);
      console.error('FAIL', version, error.message);
    }
  }
  await writeFile(path.join(folder, 'checks.json'), JSON.stringify(checks, null, 2));
  assert.deepEqual(failures, []);
} finally { await browser.close(); await server.close(); }
