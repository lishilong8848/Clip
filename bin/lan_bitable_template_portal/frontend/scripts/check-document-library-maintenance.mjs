import assert from 'node:assert/strict';
import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { checkMaintenanceSource, maintenanceFixture, output, startMaintenancePreview } from './document-library-maintenance-fixture.mjs';

// Default checks the parent's real dist. Source-only checks never build or serve an app.
if (process.argv.includes('--source-check')) {
  for (const check of await checkMaintenanceSource()) console.log(`PASS ${check}`);
} else {
  await checkDist();
}

async function checkDist() {
  const { chromium } = await import('playwright');
  const server = await startMaintenancePreview();
  const browser = await chromium.launch({ headless: true });
  const { fixture, origin } = server;
  const checks = [], errors = [];
  const viewports = [{ width: 1440, height: 900 }, { width: 402, height: 874 }, { width: 440, height: 956 }];
  let page, context;
  await mkdir(output, { recursive: true });
  const button = name => page.getByRole('button', { name, exact: true });
  const drawer = () => page.locator('.n-drawer-content').filter({ has: page.locator('.document-detail') });
  const dialog = () => page.locator('.document-maintenance-dialog');
  const summary = () => page.locator('.document-selection-summary');
  const listCalls = () => fixture.calls.filter(call => call.method === 'GET' && call.path.endsWith('/records'));
  async function open(version = 'v2', dark = false, select = true) {
    context = await browser.newContext({ viewport: viewports[0], reducedMotion: 'reduce' });
    await context.addInitScript(dark => localStorage.setItem('clipflow:workspace-night-mode:ou_fixtureDoc', dark ? 'dark' : 'light'), dark);
    await context.route('**/*', route => new URL(route.request().url()).origin === origin ? route.continue() : route.abort());
    page = await context.newPage(); page.setDefaultTimeout(12000);
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(`${origin}/?version=${version}`);
    await button('打开数据表：电气技术资料').waitFor();
    if (select) {
      await button('打开数据表：电气技术资料').click(); await button('选择本页').waitFor(); await ready();
      await page.locator('.document-filters .n-select').click();
      await page.locator('.n-base-select-option').filter({ hasText: '全部授权楼栋' }).click();
      await textIncludes(page.locator('.document-pagination'), '匹配 103 条'); await ready();
      assert.equal(listCalls().at(-1).query.scope || '', '', `${version}: maintenance checks explicitly use all authorized scopes`);
    }
  }
  async function ready() {
    await page.waitForFunction(() => !document.querySelector('.document-results[aria-busy="true"]') && !document.querySelector('.document-loading'));
  }
  async function textIncludes(locator, text) { await locator.filter({ hasText: text }).waitFor(); }
  async function stable() { await page.evaluate(async () => {
    await Promise.all(document.getAnimations().filter(animation => animation.effect?.getTiming().iterations !== Infinity).map(animation => animation.finished.catch(() => {})));
    await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
  }); }
  async function capture(name) {
    await stable();
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1), false, `${name}: horizontal overflow`);
    await page.screenshot({ path: path.join(output, name + '.png'), fullPage: true });
  }
  async function reset() { if (context) await context.close(); Object.assign(fixture, maintenanceFixture()); }
  try {
    for (const [version, dark] of [['legacy', false], ['v2', false], ['v2', true]]) {
      await reset(); await open(version, dark);
      const label = `${version}${dark ? '-dark' : ''}`;
      const rows = page.locator('.document-table tbody tr');
      assert.equal(await rows.count(), 30);
      await textIncludes(rows.nth(0), '向量化成功');
      assert.equal(await rows.nth(1).getByText('向量化成功', { exact: true }).count(), 0);
      assert.equal(await rows.nth(2).getByText('向量化成功', { exact: true }).count(), 0);
      await textIncludes(rows.nth(1), '已完成 1/2 项');
      await button('选择本页').click(); await textIncludes(summary(), '已选择 30 条');
      const beforeLast = listCalls().length;
      await button('末页资料').click(); await textIncludes(page.locator('.document-pagination'), '本页 13 条');
      assert.equal(listCalls().length - beforeLast, 1, '103 records: last page takes exactly one request');
      assert.equal(listCalls().at(-1).query.cursor, '90'); assert.equal(listCalls().at(-1).query.page_size, '30');
      await page.getByRole('checkbox', { name: '选择资料：维护资料103', exact: true }).check();
      await textIncludes(summary(), '已选择 31 条');
      await button('上一页资料').click(); await ready(); assert.equal(listCalls().at(-1).query.cursor, '60');
      await button('首页资料').click(); await ready(); assert.equal(listCalls().at(-1).query.cursor || '', '');
      await textIncludes(summary(), '已选择 31 条');
      await button('重新向量化选中').click(); await dialog().waitFor();
      assert.equal(fixture.mutations.length, 0, 'Confirmation must precede any mutation');
      for (const viewport of viewports) {
        await page.setViewportSize(viewport); await capture(`${label}-confirm-${viewport.width}x${viewport.height}`);
        const box = await dialog().boundingBox(); assert(box && box.x >= 0 && box.x + box.width <= viewport.width + 1);
      }
      await dialog().getByRole('button', { name: '取消', exact: true }).click();
      for (const viewport of viewports) {
        await page.setViewportSize(viewport); await capture(`${label}-list-${viewport.width}x${viewport.height}`);
        const checkbox = page.getByRole('checkbox', { name: '选择资料：维护资料1', exact: true }); assert(await checkbox.isVisible());
        if (viewport.width > 760) {
          const actions = rows.first().locator('.document-row-actions');
          const cell = await rows.first().locator('td').last().boundingBox();
          assert.equal(await actions.getByRole('button').count(), 2);
          for (const iconButton of await actions.getByRole('button').all()) {
            const box = await iconButton.boundingBox(), icon = await iconButton.locator('svg').boundingBox();
            assert(box && cell && box.x >= cell.x && box.x + box.width <= cell.x + cell.width + 1, `Desktop actions fit inside their cell: ${label} ${JSON.stringify({ cell, button: box })}`);
            assert(icon && icon.x >= box.x && icon.x + icon.width <= box.x + box.width + 1, 'Desktop icons are fully visible');
          }
        } else {
          const article = page.locator('.document-mobile-list article').first();
          const title = await article.locator('.document-mobile-title').boundingBox();
          const action = await article.locator('.document-mobile-heading').getByRole('button', { name: '重新向量化：维护资料1', exact: true }).boundingBox();
          assert(title && action && action.y < title.y + title.height && title.y < action.y + action.height, 'Mobile reindex shares the title row');
          assert.equal(await article.locator('.document-record-meta').getByRole('button').count(), 0, 'Mobile metadata does not add a standalone action row');
          assert.equal(await article.locator('button button').count(), 0, 'Action buttons are independent');
        }
        await checkbox.uncheck(); await checkbox.check();
        const count = listCalls().length;
        await button('末页资料').click(); await textIncludes(page.locator('.document-pagination'), '本页 13 条');
        assert.equal(listCalls().length - count, 1); await button('首页资料').click(); await ready();
      }
      await page.setViewportSize(viewports[0]);
      await button('重新向量化选中').click(); await dialog().getByRole('button', { name: '确认重新向量化', exact: true }).click();
      await textIncludes(page.locator('.document-library'), '已加入本地向量化队列');
      assert.equal(fixture.mutations.at(-1).body.record_ids.length, 31);
      assert(fixture.mutations.at(-1).body.record_ids.includes('recMaintenance103'));
      checks.push(`${label}: actual vector status, explicit cross-page selection, 103-row single-request last/home, desktop and both phone layouts`);
    }

    await reset(); await open();
    await page.getByRole('searchbox', { name: '搜索资料', exact: true }).fill('维护资料');
    await button('查询').click(); await ready();
    await page.locator('.document-filters .n-select').click();
    await page.locator('.n-base-select-option').filter({ hasText: 'A楼' }).click(); await ready();
    await button('选择全部匹配记录（跨页）').click(); await textIncludes(summary(), '65 条');
    await page.getByRole('searchbox', { name: '搜索资料', exact: true }).fill('尚未查询');
    await button('重新向量化选中').click(); await textIncludes(dialog(), '搜索：维护资料'); await textIncludes(dialog(), 'A楼');
    await dialog().getByRole('button', { name: '确认重新向量化', exact: true }).click();
    await textIncludes(page.locator('.document-library'), '65 条记录');
    const all = fixture.mutations.at(-1).body;
    assert.equal(all.all, true); assert.equal(all.q, '维护资料'); assert.equal(all.scope, 'A'); assert(!('record_ids' in all));
    await button('选择本页').click(); await button('查询').click();
    await textIncludes(summary(), '已选择 0 条');
    checks.push('all uses current applied q/scope across pages; changed filter clears selection');

    await reset(); await open();
    fixture.mutationDelay = 500; fixture.loseReceipt = true;
    await page.evaluate(() => Object.defineProperty(window.crypto, 'randomUUID', { value: undefined, configurable: true }));
    await button('重新向量化：维护资料1').click();
    const confirm = dialog().getByRole('button', { name: '确认重新向量化', exact: true });
    await confirm.evaluate(element => { element.click(); element.click(); });
    await textIncludes(dialog(), '回执传输失败');
    assert.equal(fixture.mutations.length, 1); assert.equal(fixture.applied, 1);
    const operation = fixture.mutations[0].body.operation_id;
    assert.match(operation, /^[\da-f]{8}-[\da-f]{4}-4[\da-f]{3}-[89ab][\da-f]{3}-[\da-f]{12}$/);
    await dialog().getByRole('button', { name: '按原操作重试', exact: true }).click();
    await textIncludes(page.locator('.document-library'), '已加入本地向量化队列');
    assert.equal(fixture.mutations.length, 2); assert.equal(fixture.applied, 1); assert.equal(fixture.mutations[1].body.operation_id, operation);
    checks.push('single-record confirmation, LAN HTTP UUID fallback, double-click suppression, lost-receipt stable UUID retry');

    await reset(); await open();
    fixture.listDelay = 700; fixture.cache.complete = false; fixture.cache.refreshing = true; fixture.cache.error = '部分缓存同步失败';
    const refreshStarted = page.waitForRequest(request => new URL(request.url()).pathname.endsWith('/records'));
    await button('刷新文件列表').click(); await refreshStarted;
    assert.equal(await page.locator('.document-table tbody tr').count(), 30, 'Cached rows do not disappear during refresh');
    assert.equal(await page.locator('.document-loading').count(), 0); await ready();
    await page.getByText('来源资料正在后台同步，当前显示已缓存资料。', { exact: true }).waitFor();
    assert.equal(fixture.calls.filter(call => call.path.endsWith('/schema')).at(-1).query.refresh, '1');
    await button('重试同步并读取').click(); await ready();
    assert.equal(fixture.calls.filter(call => call.method === 'POST' && call.path.endsWith('/sync')).length, 1);
    fixture.listFailure = true; await button('刷新文件列表').click(); await page.getByText('缓存列表暂时不可用', { exact: false }).waitFor();
    assert.equal(await page.locator('.document-table tbody tr').count(), 30);
    fixture.listFailure = false; fixture.listDelay = 0; fixture.empty = true;
    await button('刷新文件列表').click(); await ready();
    assert.equal(await page.getByText('没有符合条件的资料', { exact: true }).count(), 0);
    fixture.empty = false; fixture.cache.complete = true; fixture.cache.refreshing = false;
    await button('刷新文件列表').click(); await ready();
    await button('查看资料：维护资料1').click(); await drawer().getByRole('button', { name: '编辑资料', exact: true }).waitFor();
    assert.equal(fixture.calls.filter(call => call.path.endsWith('/records/recMaintenance1')).at(-1).query.refresh, undefined);
    await drawer().getByRole('button', { name: '编辑资料', exact: true }).click();
    await drawer().getByLabel('资料名称', { exact: true }).fill('保留未保存资料');
    await drawer().getByRole('button', { name: '重新读取资料', exact: true }).click();
    await drawer().getByLabel('资料名称', { exact: true }).waitFor();
    assert.equal(await drawer().getByLabel('资料名称', { exact: true }).inputValue(), '保留未保存资料');
    assert.equal(fixture.calls.filter(call => call.path.endsWith('/records/recMaintenance1')).at(-1).query.refresh, '1');
    checks.push('cached rows survive refresh/error; partial cache.error explicitly retries sync; partial empty is not confirmed empty; cached detail and refresh=1 preserve edits');

    for (const stage of ['list', 'detail']) {
      await reset(); await open();
      await button('选择本页').click(); await button('查看资料：维护资料1').click();
      await drawer().getByRole('button', { name: '编辑资料', exact: true }).waitFor();
      if (stage === 'list') {
        await drawer().getByRole('button', { name: '关闭', exact: true }).click();
        fixture.listStatus = 403; await button('刷新文件列表').click();
      } else {
        fixture.detailStatus = 403; await drawer().getByRole('button', { name: '重新读取资料', exact: true }).click();
      }
      await page.getByText(stage === 'list' ? '列表读取权限已失效' : '详情读取权限已失效', { exact: false }).waitFor();
      await drawer().waitFor({ state: 'hidden' });
      assert.equal(await page.locator('.document-table tbody tr').count(), 0);
      assert.equal(await page.locator('.document-mobile-list article').count(), 0);
      await textIncludes(summary(), '已选择 0 条');
    }
    checks.push('revoked list/detail access removes cached rows, closes old detail and clears selection');

    await reset(); await open();
    await button('选择本页').click(); await button('全部数据表').click();
    await button('打开数据表：暖通技术资料').click(); await textIncludes(summary(), '已选择 0 条');
    await button('重新向量化：维护资料1').click();
    fixture.actor = 'ou_otherFixture';
    await page.evaluate(() => window.dispatchEvent(new Event('auth-account-changed')));
    await button('打开数据表：电气技术资料').waitFor();
    assert.equal(await dialog().count(), 0); assert.equal(await page.locator('.document-detail').count(), 0);
    checks.push('table and login identity changes close stale confirmations and clear selection');

    for (const permission of ['source-denied', 'record-denied', 'legacy']) {
      await reset(); fixture.canConfigure = false;
      fixture.sourceReindex = permission !== 'source-denied'; fixture.recordReindex = permission !== 'record-denied'; fixture.legacy = permission === 'legacy';
      await open('v2', false, false);
      assert.equal(await page.getByRole('button', { name: /^移除本地接入：/ }).count(), 0);
      await button('打开数据表：电气技术资料').click(); await button('查看资料：维护资料1').waitFor();
      assert.equal(await page.getByRole('button', { name: /^重新向量化：/ }).count(), 0);
      if (permission !== 'record-denied') assert.equal(await button('重新向量化选中').count(), 0);
      if (permission === 'legacy') assert.equal(await page.getByText('向量化成功', { exact: true }).count(), 0);
    }
    await reset(); fixture.canConfigure = false; await open();
    assert(await button('重新向量化：维护资料1').isVisible(), 'Reindex is a business capability, not an admin-only action');
    checks.push('source/record ACL, admin removal gate, authorized non-admin reindex, old server compatibility');

    await reset(); await open(); fixture.loseReceipt = true;
    await button('移除本地接入：电气技术资料').click();
    await textIncludes(dialog(), '不删除飞书记录'); await textIncludes(dialog(), '不影响手动知识库文件');
    assert.equal(fixture.mutations.length, 0);
    await dialog().getByRole('button', { name: '确认移除本地接入', exact: true }).click();
    await textIncludes(dialog(), '回执传输失败');
    await dialog().getByRole('button', { name: '按原操作重试', exact: true }).click();
    await button('打开数据表：暖通技术资料').waitFor();
    await page.getByText('接入已移除，来源索引后台清理中（1 项）。', { exact: true }).waitFor();
    assert.equal(fixture.mutations.length, 2); assert.equal(fixture.applied, 1);
    assert.equal(fixture.mutations[0].body.operation_id, fixture.mutations[1].body.operation_id);
    assert(fixture.mutations.every(call => call.method === 'DELETE' && call.path === '/tables/electrical'));
    assert.equal(await button('打开数据表：电气技术资料').count(), 0);
    assert.equal(fixture.records.length, 103, 'Removing local source never deletes remote records');
    assert.equal(await page.getByText('本地接入及该来源索引已移除。', { exact: true }).count(), 0);
    fixture.cleanup_error = '清理暂未完成'; await button('刷新文件列表').click();
    await page.getByText('后台清理尚未完成：清理暂未完成', { exact: true }).waitFor();
    checks.push('remove confirmation is local-only, idempotent retry, exits removed source, pending/error cleanup is not success');
    assert.deepEqual(errors, [], 'No browser runtime errors');
    await writeFile(path.join(output, 'acceptance.json'), JSON.stringify({ checks, errors, viewports, syntheticOnly: true, actualDist: true }, null, 2));
    console.log(`Document-library maintenance: ${checks.length} groups passed; screenshots in ${output}`);
  } catch (error) {
    if (page && !page.isClosed()) await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true }).catch(() => {});
    throw error;
  } finally { await browser.close(); await server.close(); }
}
