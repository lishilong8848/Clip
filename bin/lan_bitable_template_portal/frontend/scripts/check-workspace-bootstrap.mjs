import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { chromium } from 'playwright';

const html = await readFile(new URL('../dist/assets/workspace-v2/index.html', import.meta.url), 'utf8');
let mode = 'missing', scripts = 0;
const server = createServer((request, response) => {
  const path = new URL(request.url, 'http://localhost').pathname;
  if (path.endsWith('/index.html')) {
    response.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); response.end(html);
  } else if (path.endsWith('/entry.json')) {
    response.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    response.end(JSON.stringify({ entry: mode === 'invalid' ? '../unsafe.js' : 'app-fixture.js', css: [] }));
  } else if (path.endsWith('/app-fixture.js')) {
    scripts += 1;
    response.writeHead(mode === 'healthy' ? 200 : 404, { 'Content-Type': 'text/javascript' });
    response.end(mode === 'healthy' ? "document.getElementById('app').textContent='新版已加载';" : '');
  } else {
    response.writeHead(200, { 'Content-Type': 'text/javascript' }); response.end('');
  }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const origin = 'http://127.0.0.1:' + server.address().port;
const browser = await chromium.launch({ headless: true });
try {
  const page = await browser.newPage({ viewport: { width: 390, height: 844 } });
  await page.addInitScript(() => { AbortSignal.timeout = undefined; });
  await page.goto(origin + '/assets/workspace-v2/index.html');
  await page.getByText('新版资源未完整加载。', { exact: false }).waitFor();
  const retry = page.getByRole('button', { name: '重新加载新版' });
  assert.ok((await retry.boundingBox()).height >= 44);
  assert.equal(await page.getByRole('link', { name: '返回旧版' }).getAttribute('href'), '/?frontend=legacy');
  mode = 'healthy';
  await retry.click();
  await page.getByText('新版已加载', { exact: true }).waitFor();
  const prior = scripts;
  mode = 'invalid';
  await page.reload();
  await page.getByRole('button', { name: '重新加载新版' }).waitFor();
  assert.equal(scripts, prior, 'an invalid manifest must not import its path');
  console.log('Workspace bootstrap: missing asset, explicit retry, old-browser signal support, invalid manifest: OK');
} finally {
  await browser.close();
  server.closeAllConnections();
  await new Promise(resolve => server.close(resolve));
}
