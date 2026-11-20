import assert from 'node:assert/strict';
import { chromium } from 'playwright';
import { startDocumentLibraryPreview } from './document-library-preview.mjs';

const server = await startDocumentLibraryPreview();
const browser = await chromium.launch({ headless: true });
try {
  for (const version of ['legacy', 'v2']) {
    const context = await browser.newContext();
    try {
      await context.route('**/*', route => new URL(route.request().url()).origin === server.origin ? route.continue() : route.abort());
      await context.route('**/api/auth/status*', async route => {
        const response = await route.fetch(), json = await response.json();
        // The fixture account has all its test buildings, just like the affected account.
        json.data.scope_options.unshift({ value: 'ALL', label: '全部楼栋' });
        await route.fulfill({ response, json });
      });
      const page = await context.newPage(), errors = [];
      page.on('pageerror', error => errors.push(error.message));
      const target = version === 'v2' ? '/assets/workspace-v2/index.html?scope=ALL&__page=%2Fdocument-library' : '/document-library?scope=ALL&frontend=legacy';
      const firstCall = server.fixture.calls.length;
      await page.goto(server.origin + target);
      await page.getByRole('button', { name: '打开数据表：电气技术资料', exact: true }).click();
      const pagination = version === 'v2'
        ? page.getByRole('navigation', { name: '资料分页', exact: true })
        : page.locator('.document-pagination');
      await pagination.waitFor();
      const calls = server.fixture.calls.slice(firstCall).filter(call => call.method === 'GET' && call.path.endsWith('/records'));
      assert(calls.length);
      assert.equal(calls.at(-1).query.scope || '', '', version + ': ALL must become an unrestricted authorized query');
      const records = version === 'v2'
        ? page.getByRole('article').filter({ has: page.getByRole('button', { name: /^查看资料：/ }) })
        : page.locator('.document-table tbody tr');
      assert.equal(await records.count(), 2);
      assert.equal(await page.getByText('楼栋范围无效。', { exact: true }).count(), 0);
      assert.deepEqual(errors, []);
      console.log('PASS ' + version + ': all authorized buildings display without an invalid-scope request');
    } finally {
      await context.close();
    }
  }
} finally {
  await browser.close();
  await server.close();
}
