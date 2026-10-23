import assert from 'node:assert/strict';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';
import { documentFixture, output, startDocumentLibraryPreview } from './document-library-preview.mjs';

const server = await startDocumentLibraryPreview();
const browser = await chromium.launch({ headless: true });
const checks = [], errors = [], folder = path.join(output, 'resilience');
await mkdir(folder, { recursive: true });
try {
  for (const version of ['legacy', 'v2']) {
    Object.assign(server.fixture, documentFixture());
    const fixture = server.fixture;
    fixture.dateTime = true;
    fixture.records[0].fields.fldDate = Date.parse('2026-10-23T08:35:00+08:00');
    fixture.tables[0].summary = { records: 3, pending_items: 1, updated_at: 1792376400, sync_status: 'completed', error: '' };
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, timezoneId: 'America/New_York', reducedMotion: 'reduce' });
    await context.route('**/*', route => new URL(route.request().url()).origin === server.origin ? route.continue() : route.abort());
    const page = await context.newPage(); page.on('pageerror', error => errors.push(error.message));
    const button = name => page.getByRole('button', { name, exact: true });
    const drawer = () => page.locator('.n-drawer-content').filter({ has: page.locator('.document-detail') });
    await page.goto(server.origin + '/?version=' + version);
    await page.getByText('3 条缓存记录', { exact: false }).waitFor();
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
    await drawer().getByRole('button', { name: '编辑资料', exact: true }).click();
    await drawer().getByLabel('生效日期', { exact: true }).fill('2026-10-24 09:40');
    await drawer().getByLabel('生效日期', { exact: true }).press('Enter');
    await drawer().getByLabel('资料名称', { exact: true }).click();
    await drawer().getByLabel('变更原因', { exact: true }).fill('核对日期时间');
    await drawer().getByRole('button', { name: '保存资料', exact: true }).click();
    await drawer().getByRole('button', { name: '编辑资料', exact: true }).waitFor();
    assert.equal(fixture.writes.at(-1).body.fields.fldDate, Date.parse('2026-10-24T09:40:00+08:00'));
    checks.push(version + ': date-time roundtrip uses Shanghai independent of browser timezone');

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
    checks.push(version + ': field conflicts block save until an explicit choice');

    await drawer().getByRole('button', { name: '编辑资料', exact: true }).click();
    await drawer().getByLabel('资料名称', { exact: true }).fill('刷新后核验的资料');
    await drawer().getByLabel('变更原因', { exact: true }).fill('隔离回执恢复');
    fixture.flags.loseReceipt = true;
    await drawer().getByRole('button', { name: '保存资料', exact: true }).click();
    await drawer().getByText('操作回执尚未确认，输入及原操作编号已保留。请核验后按原操作重试。', { exact: true }).waitFor();
    const applied = fixture.applied;
    page.once('dialog', dialog => dialog.accept());
    await page.reload();
    await button('打开数据表：电气技术资料').click();
    await button('核验原操作').click();
    await page.getByText('原操作已核验成功，没有重复提交。', { exact: true }).waitFor();
    assert.equal(fixture.applied, applied);
    assert.equal(fixture.pendingOperations.length, 0);
    checks.push(version + ': recovery survives reload and never repeats a mutation');

    await button('本地附件缓存').click();
    await page.getByText('可安全清理', { exact: true }).waitFor();
    await button('清理无引用缓存').click();
    await page.waitForFunction(() => document.querySelector('.document-storage')?.textContent.includes('0 个'));
    assert.equal(fixture.storage.removed, 1);
    await page.getByRole('button', { name: '关闭', exact: true }).click();
    await button('查看资料：刷新后核验的资料').click();
    for (const viewport of [{ name: 'MacBookPro', width: 1440, height: 900 }, { name: 'iPhone17Pro', width: 402, height: 874 },
        { name: 'iPhone17ProMax', width: 440, height: 956 }, { name: 'small', width: 360, height: 640 }]) {
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.evaluate(async () => { await Promise.all(document.getAnimations().filter(animation => animation.effect?.getTiming().iterations !== Infinity).map(animation => animation.finished.catch(() => {}))); await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))); });
      const overflow = await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1);
      assert.equal(overflow, false, version + ' global overflow ' + viewport.name);
      const close = await drawer().getByRole('button', { name: '关闭', exact: true }).boundingBox();
      assert(close && close.x >= 0 && close.x + close.width <= viewport.width + 1 && close.y >= 0 && close.y + close.height <= viewport.height + 1, 'Drawer actions remain visible');
      await page.screenshot({ path: path.join(folder, version + '-' + viewport.name + '.png'), fullPage: true });
    }
    checks.push(version + ': PC and phone controls remain reachable');
    await context.close();
  }
  assert.deepEqual(errors, []);
  await writeFile(path.join(folder, 'checks.json'), JSON.stringify(checks, null, 2));
  console.log(checks.join('\n'));
} finally { await browser.close(); await server.close(); }
