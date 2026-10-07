import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'node:net';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const reservation = createServer();
await new Promise(resolve => reservation.listen(0, '127.0.0.1', resolve));
const port = reservation.address().port;
await new Promise(resolve => reservation.close(resolve));
const base = 'http://127.0.0.1:' + port;
const output = path.join(root, 'output/playwright/assistant-workbench');
await mkdir(output, { recursive: true });
const server = spawn(path.join(root, 'bin/.venv/Scripts/python.exe'), ['-B', 'bin/tools/lighthouse_stream_preview.py'], {
  cwd: root, windowsHide: true, env: { ...process.env, LIGHTHOUSE_NATIVE_PREVIEW: '0', LIGHTHOUSE_PREVIEW_PORT: String(port) }, stdio: ['ignore', 'pipe', 'pipe'],
});
let log = '', browser, page;
server.stdout.on('data', value => log += value); server.stderr.on('data', value => log += value);
try {
  for (let i = 0; i < 80; i++) {
    try {
      const response = await fetch(base + '/api/health');
      if (response.ok) { assert.equal((await response.json()).instance_id, 'isolated-lighthouse-stream'); break; }
    } catch {}
    if (server.exitCode !== null || i === 79) throw new Error(log);
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  browser = await chromium.launch({ headless: true });
  page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  const errors = [], posts = [];
  let documents = 0;
  page.on('pageerror', error => errors.push(error.message));
  page.on('request', request => {
    if (request.isNavigationRequest() && request.frame() === page.mainFrame()) documents++;
    if (request.method() === 'POST' && request.url().includes('/api/assistant/messages')) posts.push(request.url());
  });
  await page.goto(base);
  await page.waitForFunction(() => document.querySelector('.module-maintenance .module-metrics')?.textContent.includes('7'));
  await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
  const input = page.getByRole('textbox', { name: '询问灯塔助手' });
  await input.waitFor();
  await page.waitForFunction(() => !document.querySelector('.composer textarea')?.disabled);
  await page.evaluate(() => { window.fixtureAssistant = document.querySelector('.lighthouse'); window.fixtureComposer = document.querySelector('.composer textarea'); });
  const mask = await page.locator('.assistant-launcher svg mask').getAttribute('id');
  await input.fill('导航时继续这次回答');
  await page.getByRole('button', { name: '发送问题', exact: true }).click();
  await page.locator('.module-maintenance .module-card__main').click();
  await page.locator('.scope-card').getByRole('button', { name: '进入维护管理', exact: true }).click();
  const native = page.frameLocator('iframe[title="通告管理"]');
  await native.locator('.workspace').waitFor();
  assert.equal(await native.locator('.assistant-launcher').count(), 0, 'no duplicate assistant inside workbench');
  assert.equal(documents, 1, 'entering workbench must not reload the top-level page');
  assert.equal(await page.locator('.assistant-launcher svg mask').getAttribute('id'), mask, 'same bot renderer');
  assert(await page.evaluate(() => document.querySelector('.lighthouse') === window.fixtureAssistant && document.querySelector('.composer textarea') === window.fixtureComposer), 'same assistant and input nodes across navigation');
  await page.waitForFunction(() => (document.querySelector('.assistant .answer') || document.querySelector('.answer'))?.textContent.includes('未完成维修单'));
  assert.equal(posts.length, 1, 'navigation never resubmits the original question');

  await native.locator('.notice-row').first().click();
  await native.locator('.notice-detail-overlay.open').waitFor();
  await input.fill('抽屉内外仍可使用助手');
  await input.press('Tab');
  assert(await page.evaluate(() => !!document.activeElement?.closest('.lighthouse')), 'Tab remains in the assistant, not the drawer');
  await page.screenshot({ path: path.join(output, 'workbench-with-assistant.png') });

  // The parent's native top layer must remain interactive even with sibling dialogs.
  await page.evaluate(() => {
    for (const id of ['earlier-dialog', 'later-dialog']) {
      const dialog = document.createElement('dialog'); dialog.id = id;
      dialog.innerHTML = '<p>隔离原生弹窗</p>'; document.body.append(dialog);
    }
    document.getElementById('later-dialog').showModal();
    document.getElementById('earlier-dialog').showModal();
  });
  await page.waitForFunction(() => document.getElementById('earlier-dialog').querySelector('.lighthouse-layer'));
  await input.fill('原生弹窗上仍可输入');
  await input.press('Tab');
  assert(await page.evaluate(() => !!document.activeElement?.closest('.lighthouse')), 'native dialog does not make assistant inert');
  await page.screenshot({ path: path.join(output, 'native-modal-with-assistant.png') });
  await page.evaluate(() => { for (const id of ['earlier-dialog', 'later-dialog']) { const dialog = document.getElementById(id); dialog.close(); dialog.remove(); } });
  await input.fill('');
  await page.getByRole('button', { name: '收起助手', exact: true }).click();
  await page.locator('.assistant-panel').waitFor({ state: 'detached' });
  await native.getByRole('button', { name: '关闭当前通告', exact: true }).click();
  await native.locator('.notice-detail-overlay.open').waitFor({ state: 'hidden' });
  await native.locator('#lite-back-link').click();
  await page.waitForURL('**/?entry=maintenance');
  await page.locator('.scope-card').first().waitFor();
  assert.equal(documents, 1, 'returning to home must not reload the page');
  assert((await page.locator('.scope-summary-strip').textContent()).includes('7'), 'cached overview renders without waiting for refresh');
  assert.equal(await page.locator('.assistant-launcher svg mask').getAttribute('id'), mask);

  await page.goto(base + '/workbench-lite?scope=D&work_type=maintenance');
  await page.frameLocator('iframe[title="通告管理"]').locator('.workspace').waitFor();
  assert.equal(await page.locator('.lighthouse-launcher').count(), 1, 'direct bookmark still has one assistant');
  assert.deepEqual(errors, []);
  console.log('[pass] same conversation across workbench navigation, streaming without resubmission, drawer/native dialog use, cached homepage, direct bookmark');
} catch (error) {
  if (page) await page.screenshot({ path: path.join(output, 'failure.png') });
  console.error(log.slice(-6000)); throw error;
} finally {
  await browser?.close();
  if (server.exitCode === null) { const ended = once(server, 'exit'); server.kill(); await ended; }
}
