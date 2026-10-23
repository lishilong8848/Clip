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
      const target = version === 'v2' ? '/assets/workspace-v2/index.html?scope=ALL&__page=%2Fdocument-library' : '/document-library?scope=ALL';
      await page.goto(server.origin + target);
      await page.getByRole('button', { name: '打开数据表：电气技术资料', exact: true }).click();
      await page.locator('.document-pagination').waitFor();
      const calls = server.fixture.calls.filter(call => call.method === 'GET' && call.path.endsWith('/records'));
      assert(calls.length);
      assert.equal(calls.at(-1).query.scope || '', '', version + ': ALL must become an unrestricted authorized query');
      assert.equal(await page.locator('.document-table tbody tr').count(), 2);
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
