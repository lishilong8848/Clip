import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { chromium } from 'playwright';

const base = 'http://127.0.0.1:19003';
const output = path.resolve('../../../output/playwright/assistant-secure-pages');
await mkdir(output, { recursive: true });
const browser = await chromium.launch({ headless: true });
try {
  for (const width of [1440, 390]) {
    for (const admin of [true, false]) {
      const context = await browser.newContext({ viewport: { width, height: 1000 } });
      await context.addCookies([{ name: 'fixture_user', value: admin ? 'ALL' : 'D', url: base }]);
      assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
      const page = await context.newPage(), errors = [], writes = [], permissionReads = [];
      page.on('pageerror', error => errors.push(error.message));
      page.on('request', request => {
        if (request.url().includes('/api/') && request.method() !== 'GET') writes.push(request.url());
        if (request.url().includes('/api/auth/permissions')) permissionReads.push(request.url());
      });
      for (const tab of ['status', 'handover', 'permissions']) {
        await page.goto(base + '/?admin=' + tab);
        await page.locator('.app-shell').waitFor();
        if (admin) {
          const pane = page.locator(tab === 'status' ? '.admin-shell .pane' : tab === 'handover' ? '.admin-handover-pane' : '.permission-pane');
          await pane.waitFor();
          if (tab === 'status') await pane.getByRole('button', { name: '刷新状态', exact: true }).waitFor();
          if (tab === 'handover') assert.equal(await pane.locator('input[type=password]').count(), 1);
          assert.equal(await page.locator('.lighthouse-panel input[type=password]').count(), 0);
          await page.screenshot({ path: path.join(output, `${tab}-${width}.png`) });
          await page.locator('.admin-shell').getByRole('button', { name: '关闭', exact: true }).click();
          await page.locator('.admin-shell').waitFor({ state: 'hidden' });
          assert.equal(new URL(page.url()).searchParams.has('admin'), false);
          await page.reload();
        } else {
          await page.getByText('隔离测试D', { exact: true }).first().waitFor();
        }
        assert.equal(await page.locator('.admin-shell').count(), 0);
      }
      assert.deepEqual(writes, []);
      if (!admin) assert.deepEqual(permissionReads, []);
      assert.deepEqual(errors, []);
      await context.close();
      console.log(`Secure page links ${width}px ${admin ? 'admin' : 'building'}: native forms, close/reload, no writes OK`);
    }
  }
} finally {
  await browser.close();
}
